import pandas as pd
import numpy as np

file_list = [
    r'C:\FPT Polytechnic\Project Tự Làm\Điểm thi thpt 2020 - 2024\diem_thi_2020_2021.csv',
    r'C:\FPT Polytechnic\Project Tự Làm\Điểm thi thpt 2020 - 2024\diem_thi_thpt_2022.csv',
    r'C:\FPT Polytechnic\Project Tự Làm\Điểm thi thpt 2020 - 2024\diem_thi_thpt_2023.csv',
    r'C:\FPT Polytechnic\Project Tự Làm\Điểm thi thpt 2020 - 2024\diem_thi_thpt_2024.csv' 
]
df_national_examination_board = pd.read_excel(r"C:\FPT Polytechnic\Project Tự Làm\Điểm thi thpt 2020 - 2024\danh_sach_hoi_dong_thi.xlsx")

class NationalHighSchoolExamScore:
    def __init__(self, file_paths,df_national_examination_board):
        self.file_paths = file_paths
        self.dataframes = []
        self.df_national_examination_board = df_national_examination_board

    def read_data(self):
        for path in self.file_paths:
            try:
                df = pd.read_csv(path)
                self.dataframes.append((path, df))
                print(f"Complete reading the file: {path}")
            except Exception as e:
                print(f"Unable to read file {path}: {e}")

    def check_data(self):
        if not self.dataframes:
            print("No data available")
            return

        for (path, df) in self.dataframes:
            print(f"\nCheck data: {path}")
            print(f"Row numbers: {df.shape[0]}, Column numbers: {df.shape[1]}")
            print("Columns:", list(df.columns))
            print("Duplicate Values:", df.duplicated().sum())
            print("-" * 60)

    def remove_duplicate(self):
        if not self.dataframes:
            print("No data to process.")
            return

        for i, (path, df) in enumerate(self.dataframes):
            before = df.shape[0]
            df_cleaned = df.drop_duplicates()
            after = df_cleaned.shape[0]
            self.dataframes[i] = (path, df_cleaned)
            print(f"\nRemove {before - after} duplicate rows from file: {path}")
            
    def drop_specific_columns(self, columns_to_drop_by_file):
        if not self.dataframes:
            print("No data to process.")
            return
        
        for i, (path, df) in enumerate(self.dataframes):
            for keyword, columns in columns_to_drop_by_file.items():
                if keyword in path:
                    existing_cols = [col for col in columns if col in df.columns]
                    df = df.drop(columns=existing_cols)
                    self.dataframes[i] = (path, df)
                    print(f"\nDropped columns from file {path}: {existing_cols if existing_cols else 'No matching columns found'}")
                    break 
    def rename_columns(self, rename_rules_by_file):
        if not self.dataframes:
            print("No data to process.")
            return

        for i, (path, df) in enumerate(self.dataframes):
            for keyword, rename_map in rename_rules_by_file.items():
                if keyword in path:
                    existing_renames = {old: new for old, new in rename_map.items() if old in df.columns}
                    df = df.rename(columns=existing_renames)
                    self.dataframes[i] = (path, df)
                    print(f"\nRenamed columns in file {path}: {existing_renames if existing_renames else 'No matching columns to rename'}")
                    break
    def add_column_code_year_khtn_khxh(self):
        if not self.dataframes:
            print("No data to process.")
            return

        start_year = 2020

        for i, (path, df) in enumerate(self.dataframes):
            print(f"\nProcessing file: {path}")

            # Thêm cột 'code' nếu chưa có
            if 'code' not in df.columns:
                if 'sbd' in df.columns:
                    try:
                        df['code'] = df['sbd'].astype(str).str[:2].astype(int)
                        print("Added column 'code'")
                    except Exception as e:
                        print(f"Error adding 'code': {e}")
                else:
                    print("'sbd' column not found.")
            else:
                print("'code' already exists.")

            # Thêm cột 'year' nếu chưa có
            if 'year' not in df.columns:
                df['year'] = start_year + i
                print(f"Added column 'year' = {start_year + i}")

            # Thêm cột 'khtn'
            cols_khtn = ['vat_li', 'hoa_hoc', 'sinh_hoc']
            if all(col in df.columns for col in cols_khtn):
                df['khtn'] = df[cols_khtn].sum(axis=1)
                df['khtn'] = df['khtn'].where(df[cols_khtn].notnull().all(axis=1))
                print("Added column 'khtn'")
            else:
                print(f"Missing KHTN columns: {', '.join([col for col in cols_khtn if col not in df.columns])}")

            # Thêm cột 'khxh'
            cols_khxh = ['lich_su', 'dia_li', 'gdcd']
            if all(col in df.columns for col in cols_khxh):
                df['khxh'] = df[cols_khxh].sum(axis=1)
                df['khxh'] = df['khxh'].where(df[cols_khxh].notnull().all(axis=1))
                print("Added column 'khxh'")
            else:
                print(f"Missing KHXH columns: {', '.join([col for col in cols_khxh if col not in df.columns])}")

            # Cập nhật lại
            self.dataframes[i] = (path, df)



    def check_data_column_year(self):
        if not self.dataframes:
            print("No data to process")
            return
        for path, df in self.dataframes:
            print(df["year"].unique())

    def reorder_all_columns(self):
            if not self.dataframes:
                print("No data to process.")
                return

            desired_order = [
                'sbd', 'toan', 'ngu_van', 'vat_li', 'hoa_hoc', 'sinh_hoc',
                'lich_su', 'dia_li', 'gdcd', 'ngoai_ngu','code', 'khtn',
                'khxh', 'year'
            ]

            for i, (path, df) in enumerate(self.dataframes):
                existing = [col for col in desired_order if col in df.columns]
                remaining = [col for col in df.columns if col not in desired_order]
                new_order = existing + remaining
                df = df[new_order]
                self.dataframes[i] = (path, df)
                print(f"Successfully reordered columns for the file: {path}")
        
    def concat_all(self, save_path="C:\FPT Polytechnic\Project Tự Làm\Điểm thi thpt 2020 - 2024\processed.csv"):
        if not self.dataframes:
            print("No data available to concatenate.")
            return None

        try:
            all_dfs = [df for _, df in self.dataframes]
            df_concat = pd.concat(all_dfs, ignore_index=True)
            df_concat.to_csv(save_path, index=False, encoding='utf-8-sig')
            print(f"Successfully concatenated {len(all_dfs)} DataFrames.")
            print(f"File saved to: {save_path}")
            return df_concat
        except Exception as e:
            print(f"Error during concatenation: {e}")
            return None



def main():
    data = NationalHighSchoolExamScore(file_list,df_national_examination_board)
    data.read_data()

    data.check_data()

    data.remove_duplicate()

    columns_to_drop = {
    "2020_2021": ["Tên", "Ngày Sinh", "Giới tính"],
    "2023": ["ma_ngoai_ngu"],
    "2024": ["ma_ngoai_ngu"]
    }
    data.drop_specific_columns(columns_to_drop)

    data.rename_columns({
    "2020_2021": {"SBD": "sbd", "Toán": "toan", "Văn": "ngu_van", "Ngoại Ngữ": "ngoai_ngu", "Lý": "vat_li", "Hoá": "hoa_hoc", 
                  "Sinh": "sinh_hoc", "Lịch Sử": "lich_su", "Địa Lý": "dia_li", "GDCD": "gdcd", "Year": "year"}
    })

    data.add_column_code_year_khtn_khxh()

    data.check_data_column_year()
    data.drop_specific_columns({
    "2020_2021": ["province"]
    })

    data.reorder_all_columns()
    data.check_data()
    df_national_high_school_exam_score = data.concat_all()

    df_national_examination_board.info()
    df_national_examination_board.rename(columns={
    "Mã hội đồng": "code",
    "Tên hội đồng thi": "national examination board",
    "Tên Tỉnh": "province"
    }, inplace=True)

    df_national_examination_board.to_csv(r"C:\FPT Polytechnic\Graduation_Project\Data\Processed\national_examination_board.csv", index=False)
    df_region_of_vietnam = pd.read_excel(r"C:\FPT Polytechnic\Graduation_Project\Data\Raw\Regions_of_VietNam.xlsx")
    df_national_examination_board_transform = pd.merge(df_national_examination_board, df_region_of_vietnam, how="left", left_on='province', right_on='Province')
    df_national_examination_board_transform.info()
    df_national_examination_board_transform.drop(columns='Province', inplace=True)
    df_national_examination_board_transform.rename(columns={'Regions': 'regions'}, inplace=True)
    df_national_examination_board_transform.to_csv(r'C:\FPT Polytechnic\Graduation_Project\Data\Processed\national_examination_board_transform.csv', index=False)
    df_national_high_school_exam_score.info()




if __name__ == "__main__":
    main()

