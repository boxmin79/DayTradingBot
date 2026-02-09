# pickle로 저장된 파일을 CSV 파일로 변환하는 스크립트
import pandas as pd
import os
import sys

# data/chart/minute 폴더 경로 설정
base_dir = os.getcwd()
minute_dir = os.path.join(base_dir, "data", "chart", "minute")

def convert_pkl_to_csv(minute_dir):
    if not os.path.exists(minute_dir):
        print(f"!!! [오류] 지정된 폴더가 없습니다: {minute_dir}")
        return

    pkl_files = [f for f in os.listdir(minute_dir) if f.endswith('.pkl')]
    
    if not pkl_files:
        print("!!! [오류] 변환할 PKL 파일이 없습니다.")
        return

    for pkl_file in pkl_files:
        pkl_path = os.path.join(minute_dir, pkl_file)
        csv_path = os.path.join(minute_dir, pkl_file.replace('.pkl', '.csv'))
        
        try:
            # PKL 파일 로드
            df = pd.read_pickle(pkl_path)
            # CSV 파일로 저장
            df.to_csv(csv_path, index=False)
            print(f"[완료] {pkl_file} -> {pkl_file.replace('.pkl', '.csv')}")
        except Exception as e:
            print(f"!!! {pkl_file} 변환 중 에러: {e}")
            continue

if __name__ == "__main__":
    convert_pkl_to_csv(minute_dir)
    sys.exit(0)
    