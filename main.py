# main.py
import os, io, time, tempfile, datetime as dt
from flask import Flask, request, jsonify, make_response
import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from google.cloud import storage

# ===== 環境変数 =====
# 例: GCS_BUCKET=my-bucket PREDIX_DIR=predictions RESULT_DIR=results
GCS_BUCKET = os.environ.get("GCS_BUCKET", "")
PREDIX_DIR = os.environ.get("PREDIX_DIR", "predictions")  # 予測Excelのベースパス
RESULT_DIR = os.environ.get("RESULT_DIR", "results")      # 出力のベースパス
TZ = dt.timezone(dt.timedelta(hours=9))                   # Asia/Tokyo

app = Flask(__name__)
storage_client = storage.Client()

# ===== スクレイパ =====
class Return:
    @staticmethod
    def scrape(race_id_list):
        return_tables = {}
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/112.0.0.0 Safari/537.36"
            )
        }
        for race_id in tqdm(race_id_list):
            time.sleep(1)  # サイト負荷配慮
            try:
                url = f"https://nar.netkeiba.com/race/result.html?race_id={race_id}&rf=race_list"
                resp = requests.get(url, headers=headers, timeout=30)
                resp.encoding = "EUC-JP"
                html = resp.text.replace("<br />", "br")

                # テーブルがなければスキップ
                if "<table" not in html:
                    print(f"[WARN] {race_id}: no table")
                    continue

                # まずは read_html で
                try:
                    dfs = pd.read_html(html)
                    # 通常は dfs[1], dfs[2] を結合
                    df = pd.concat([dfs[1], dfs[2]], ignore_index=True)
                except Exception:
                    # フォールバック：手動パース
                    soup = BeautifulSoup(html, "html.parser")
                    tbl = soup.find("table", {"summary": "払い戻し"})
                    if not tbl:
                        print(f"[WARN] {race_id}: no manual table")
                        continue
                    cols = [th.get_text(strip=True) for th in tbl.find_all("th")]
                    rows = [
                        [td.get_text(strip=True).replace("br", "\n") for td in tr.find_all("td")]
                        for tr in tbl.find_all("tr") if tr.find_all("td")
                    ]
                    if not rows:
                        print(f"[WARN] {race_id}: no rows")
                        continue
                    df = pd.DataFrame(rows, columns=cols)

                df["race_id"] = race_id
                return_tables[race_id] = df

            except Exception as e:
                print(f"[ERROR] {race_id}: {e}")
                continue

        if not return_tables:
            return pd.DataFrame()
        return pd.concat(return_tables.values(), ignore_index=True)

# ===== 会場コード→会場名（judge用） =====
venue_map = {
    "01":"札幌","02":"函館","03":"福島","04":"新潟","05":"東京","06":"中山","07":"中京","08":"京都","09":"阪神","10":"小倉",
    "42":"浦和","43":"船橋","44":"大井","45":"川崎","65":"帯広","30":"門別","35":"盛岡","36":"水沢","47":"笠松","48":"名古屋",
    "50":"園田","54":"高知","55":"佐賀","46":"金沢","51":"姫路",
}
important_marks = ["◎","○","▲"]
all_marks = ["◎","○","▲","△","☆","注"]

def parse_race_id(race_id: str):
    venue_code = race_id[4:6]  # yyyy(4) + venue(2)
    race_no = int(race_id[-2:])
    return venue_map.get(venue_code, "不明"), f"{race_no}R"

def judge_hits(result_df: pd.DataFrame, excel_path: str) -> str:
    # 入ってくる列順を安全に揃える
    result_df = result_df.rename(columns={
        result_df.columns[0]: "baken_types",
        result_df.columns[1]: "horse_number",
        result_df.columns[2]: "refund",
        result_df.columns[3]: "popularity",
        result_df.columns[-1]: "race_id",
    })[["race_id", "baken_types", "horse_number", "refund", "popularity"]]

    xls = pd.ExcelFile(excel_path)
    sheet_names = xls.sheet_names
    outputs = []

    for race_id in result_df["race_id"].unique():
        venue, race_no = parse_race_id(race_id)
        target_sheet = next((s for s in sheet_names if venue in s and race_no in s), None)
        if target_sheet is None:
            print(f"[WARN] {race_id} ({venue}{race_no}) sheet missing")
            continue

        df_pred = pd.read_excel(excel_path, sheet_name=target_sheet)
        mark_dict = dict(zip(df_pred["馬番"].astype(str), df_pred["印"].fillna("")))

        race_df = result_df[result_df["race_id"] == race_id]
        race_out = [f"{venue}{race_no}"]

        for _, row in race_df.iterrows():
            baken = row["baken_types"]
            if baken in ["枠連", "枠単"]:
                continue

            horses = str(row["horse_number"]).split()
            refunds = str(row["refund"]).split("br")

            if baken == "単勝":
                if horses and mark_dict.get(horses[0], "") == "◎":
                    race_out.append(f"{baken}：{horses[0]} ◎：{refunds[0] if refunds else ''}")

            elif baken == "複勝":
                for h, r in zip(horses, refunds):
                    if mark_dict.get(h, "") == "◎":
                        race_out.append(f"{baken}：{h} ◎：{r}")

            elif baken == "ワイド":
                pairs = [horses[i:i+2] for i in range(0, len(horses), 2)]
                for (pair, r) in zip(pairs, refunds):
                    marks = [mark_dict.get(h, "") for h in pair]
                    if all(m in all_marks for m in marks) and any(m in important_marks for m in marks):
                        race_out.append(f"{baken}：{'-'.join(pair)} {''.join(marks)}：{r}")

            elif baken in ["馬連", "3連複"]:
                marks = [mark_dict.get(h, "") for h in horses]
                if all(m in all_marks for m in marks) and any(m in important_marks for m in marks):
                    race_out.append(f"{baken}：{'-'.join(horses)} {''.join(marks)}：{refunds[0] if refunds else ''}")

            elif baken in ["馬単", "3連単"]:
                marks = [mark_dict.get(h, "") for h in horses]
                if all(m in all_marks for m in marks) and any(m in important_marks for m in marks):
                    race_out.append(f"{baken}：{'→'.join(horses)} {''.join(marks)}：{refunds[0] if refunds else ''}")

        if len(race_out) > 1:
            outputs.append("\n".join(race_out))

    return "\n\n".join(outputs)

# ===== GCS helpers =====
def gcs_blob(path: str):
    bucket = storage_client.bucket(GCS_BUCKET)
    return bucket.blob(path)

def save_df_to_gcs_csv(df: pd.DataFrame, path: str):
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    gcs_blob(path).upload_from_string(buf.getvalue(), content_type="text/csv")

def save_text_to_gcs(text: str, path: str):
    gcs_blob(path).upload_from_string(text, content_type="text/plain; charset=utf-8")

def download_gcs_to_tmp(path: str) -> str:
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(path)[1])
    gcs_blob(path).download_to_filename(tmp.name)
    return tmp.name

# ===== 実行エンドポイント =====
@app.get("/run")
def run():
    # --- 日付（デフォルトは JST の当日。?date=YYYYMMDD or YYYY-MM-DD で指定可） ---
    now = dt.datetime.now(TZ)
    date_str = request.args.get("date")
    if date_str:
        date_key = date_str.replace("-", "")
        when = dt.datetime.strptime(date_key, "%Y%m%d").replace(tzinfo=TZ)
    else:
        date_key = now.strftime("%Y%m%d")
        when = now

    yyyy = when.strftime("%Y")
    mmdd = when.strftime("%m%d")

    # --- race_id 規則: yyyy + 会場コード(2桁) + mmdd + 01〜12 ---
    # 必要な会場コードをここに並べてください
    venue_codes = ["42", "43", "44", "45"]  # 例: 盛岡=35, 門別=30, 名古屋=48, 園田=50
    target_ids = [
        f"{yyyy}{vc}{mmdd}{str(i).zfill(2)}"
        for vc in venue_codes
        for i in range(1, 12 + 1)
    ]

    # 払い戻しスクレイプ
    results = Return.scrape(target_ids)

    # 出力先ディレクトリ
    out_dir = f"{RESULT_DIR}/{date_key}"
    csv_path = f"{out_dir}/pay_results_{date_key}.csv"

    if results.empty:
        # 痕跡として空CSVを保存
        save_df_to_gcs_csv(pd.DataFrame(), csv_path)
        return make_response(jsonify({
            "status": "no_data",
            "date": date_key,
            "csv": f"gs://{GCS_BUCKET}/{csv_path}",
            "txt": []
        }), 200)

    # CSV保存
    save_df_to_gcs_csv(results, csv_path)

    # --- 3つのExcelを順に判定 ---
    # 例: gs://<bucket>/predictions/YYYYMMDD/◯◯◯_予測結果_YYYYMMDD.xlsx
    predict_sources = [
        {"label": "競馬大学", "filename": f"競馬大学_予測結果_{date_key}.xlsx"},
        {"label": "IQ150",  "filename": f"IQ150_予測結果_{date_key}.xlsx"},
        {"label": "尻子",    "filename": f"尻子_予測結果_{date_key}.xlsx"},
    ]

    txt_outputs = []
    for src in predict_sources:
        excel_gcs_path = f"{PREDIX_DIR}/{date_key}/{src['filename']}"
        try:
            excel_local = download_gcs_to_tmp(excel_gcs_path)
        except Exception as e:
            print(f"[WARN] Excel not found: gs://{GCS_BUCKET}/{excel_gcs_path} ({e})")
            txt_outputs.append({
                "label": src["label"],
                "status": "excel_missing",
                "excel": f"gs://{GCS_BUCKET}/{excel_gcs_path}"
            })
            continue

        try:
            txt = judge_hits(results, excel_local)
        except Exception as e:
            print(f"[ERROR] judge_hits failed for {src['label']}: {e}")
            txt_outputs.append({
                "label": src["label"],
                "status": "judge_error",
                "excel": f"gs://{GCS_BUCKET}/{excel_gcs_path}"
            })
            continue

        # 保存先: results/YYYYMMDD/<ラベル>_的中結果.txt
        txt_path = f"{out_dir}/{src['label']}_的中結果.txt"
        save_text_to_gcs(txt, txt_path)

        txt_outputs.append({
            "label": src["label"],
            "status": "ok",
            "txt": f"gs://{GCS_BUCKET}/{txt_path}",
            "excel": f"gs://{GCS_BUCKET}/{excel_gcs_path}"
        })

    return jsonify({
        "status": "ok",
        "date": date_key,
        "csv": f"gs://{GCS_BUCKET}/{csv_path}",
        "txt": txt_outputs
    })


if __name__ == "__main__":
    # ローカル検証用（Cloud Runでは不要）
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
