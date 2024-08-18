from src.config import load_config
from src.data_loader import load_all_data
from src.analytics import *
import os
import pandas as pd


def main():
    print("In main")
    config = load_config("config/config.yaml")
    data = load_all_data(config)

    result_1 = analysis_1(data["primary_person"])
    result_2 = analysis_2(data["units"])
    result_3 = analysis_3(data["primary_person"], data["units"])
    result_4 = analysis_4(data["primary_person"], data["units"])
    result_5 = analysis_5(data["primary_person"])
    result_6 = analysis_6(data["primary_person"], data["units"])
    result_7 = analysis_7(data["primary_person"], data["units"])
    result_8 = analysis_8(data["primary_person"], data["units"])
    result_9 = analysis_9(data["damages"], data["units"])
    result_10 = analysis_10(data["charges"], data["primary_person"], data["units"])

    out_dir = config["output_paths"]["analytics_results"]
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    result_1.write.csv(f"{out_dir}analysis_1.csv", header=True, mode="overwrite")
    result_2.write.csv(f"{out_dir}analysis_2.csv", header=True, mode="overwrite")
    result_3.write.csv(f"{out_dir}analysis_3.csv", header=True, mode="overwrite")
    result_4.write.csv(f"{out_dir}analysis_4.csv", header=True, mode="overwrite")
    result_5.write.csv(f"{out_dir}analysis_5.csv", header=True, mode="overwrite")
    result_6.write.csv(f"{out_dir}analysis_6.csv", header=True, mode="overwrite")
    result_7.write.csv(f"{out_dir}analysis_7.csv", header=True, mode="overwrite")
    result_8.write.csv(f"{out_dir}analysis_8.csv", header=True, mode="overwrite")
    result_9.write.csv(f"{out_dir}analysis_9.csv", header=True, mode="overwrite")
    result_10.write.csv(f"{out_dir}analysis_10.csv", header=True, mode="overwrite")


if __name__ == '__main__':
    main()

