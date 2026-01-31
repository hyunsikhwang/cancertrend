import shutil
import os

files_to_copy = [
    ("/Users/hyunsikhwang/.gemini/antigravity/brain/4f8d9901-450d-4f63-9e1c-295f181cdb48/app_logo_final_1769840116895.png", "/Users/hyunsikhwang/cancertrend/logo.png"),
    ("/Users/hyunsikhwang/.gemini/antigravity/brain/4f8d9901-450d-4f63-9e1c-295f181cdb48/trend_icon_final_1769840130805.png", "/Users/hyunsikhwang/cancertrend/trend_icon.png"),
    ("/Users/hyunsikhwang/.gemini/antigravity/brain/4f8d9901-450d-4f63-9e1c-295f181cdb48/ranking_icon_final_1769840144372.png", "/Users/hyunsikhwang/cancertrend/ranking_icon.png")
]

for src, dst in files_to_copy:
    try:
        shutil.copy2(src, dst)
        print(f"Successfully copied {src} to {dst}")
    except Exception as e:
        print(f"Failed to copy {src}: {e}")

# Verify
print("Verifying files in /Users/hyunsikhwang/cancertrend/")
for _, dst in files_to_copy:
    if os.path.exists(dst):
        print(f"Exists: {dst}")
    else:
        print(f"Does NOT exist: {dst}")
