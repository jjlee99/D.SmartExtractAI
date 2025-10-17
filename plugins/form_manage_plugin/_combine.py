import os

def combine_files_to_one(source_dir, output_file, extensions=None, except_files=None):
    """
    source_dir: 소스 파일들이 들어있는 최상위 폴더 경로
    output_file: 결과를 저장할 텍스트 파일 경로
    extensions: 읽을 파일 확장자 리스트(None이면 모든 파일)
    except_files: 제외할 파일명 리스트
    """
    except_files = except_files or []
    with open(output_file, 'w', encoding='utf-8') as outfile:
        for root, dirs, files in os.walk(source_dir):
            for filename in files:
                # 예외 파일명 체크
                if filename in except_files:
                    continue
                # 확장자 필터링
                if extensions and not any(filename.endswith(ext) for ext in extensions):
                    continue

                file_path = os.path.join(root, filename)
                # 파일명 구분 헤더 (# 파일명)
                outfile.write(f"\n{'='*40}\n# {os.path.relpath(file_path, source_dir)}\n{'='*40}\n")
                try:
                    with open(file_path, 'r', encoding='utf-8') as infile:
                        content = infile.read()
                        outfile.write(content)
                        outfile.write('\n')
                except Exception as e:
                    print(f"Error reading {file_path}: {e}")


if __name__ == "__main__":
    relative_path = "plugins/form_manage_plugin"  # vscode에서 폴더의 cope relative path 후 붙여넣기
    source_directory = f"./{relative_path}"  # 소스가 모여있는 폴더 경로로 변경 plugins/form_manage_plugin
    output_txt_file = "combined_source.txt"  # 출력 파일명 변경 가능
    file_extensions = ['.py']  # 필요한 확장자만 필터링 가능
    except_files = ['_combine.py']  # 예외 파일명 리스트

    combine_files_to_one(source_directory, output_txt_file, file_extensions, except_files)
    print("파일 병합 완료")
