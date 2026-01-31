import os
import shutil
import sys

src_dir = "/Users/hyunsikhwang/.gemini/antigravity/brain/4f8d9901-450d-4f63-9e1c-295f181cdb48/"
dst_dir = "/Users/hyunsikhwang/cancertrend/"

print(f"Listing source directory: {src_dir}")
try:
    files = os.listdir(src_dir)
    print(f"Files found: {files}")
except Exception as e:
    print(f"Error listing source: {e}")

mapping = {
    "app_logo_final_1769840116895.png": "logo.png",
    "trend_icon_final_1769840130805.png": "trend_icon.png",
    "ranking_icon_final_1769840144372.png": "ranking_icon.png"
}

for src_name, dst_name in mapping.items():
    src_path = os.path.join(src_dir, src_name)
    dst_path = os.path.join(dst_dir, dst_name)
    print(f"Attempting to copy {src_path} to {dst_path}")
    try:
        shutil.copy2(src_path, dst_path)
        print("Success!")
    except Exception as e:
        print(f"Failure: {e}")

print("Final listing of destination:")
print(os.listdir(dst_dir))
sys.stdout.flush()
