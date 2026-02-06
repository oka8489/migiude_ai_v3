"""
後方互換: app.py は 工事登録.py にリネームされました。
streamlit run app.py で起動する場合は、工事登録.py を実行します。
"""
import runpy
runpy.run_path("工事登録.py", run_name="__main__")
