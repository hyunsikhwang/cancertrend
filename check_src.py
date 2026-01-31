import os

src_path = "/Users/hyunsikhwang/.gemini/antigravity/brain/4f8d9901-450d-4f63-9e1c-295f181cdb48/app_logo_final_1769840116895.png"
print(f"Checking if src exists: {os.path.exists(src_path)}")

contents = os.listdir("/Users/hyunsikhwang/.gemini/antigravity/brain/4f8d9901-450d-4f63-9e1c-295f181cdb48/")
print(f"Brain dir contents: {contents}")
