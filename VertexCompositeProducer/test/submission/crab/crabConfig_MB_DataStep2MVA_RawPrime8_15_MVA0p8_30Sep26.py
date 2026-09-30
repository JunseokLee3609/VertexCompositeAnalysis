from pathlib import Path
import runpy

common = runpy.run_path(str(Path(__file__).resolve().with_name("data_mva08_30Sep26_common.py")))
config = common["make_config"](8, 15)
