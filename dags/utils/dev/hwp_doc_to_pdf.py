import os
import subprocess

def convert_hwp_to_pdf(input_path, output_dir):
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file {input_path} not found")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    cmd = ["libreoffice", "--headless", "--convert-to", "pdf", input_path, "--outdir", output_dir]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"Conversion success: {input_path} -> {output_dir}")
    else:
        print(f"Conversion failed: {result.stderr}")

def convert_docx_to_pdf(input_path, output_dir):
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file {input_path} not found")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    cmd = ["libreoffice", "--headless", "--convert-to", "pdf", input_path, "--outdir", output_dir]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"Conversion success: {input_path} -> {output_dir}")
    else:
        print(f"Conversion failed: {result.stderr}")

# 사용 예시
convert_hwp_to_pdf("test.hwp", "./converted")

# 사용 예시
convert_docx_to_pdf("test.docx", "./converted")

