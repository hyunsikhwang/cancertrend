import os

src_path = "/Users/hyunsikhwang/.gemini/antigravity/brain/4f8d9901-450d-4f63-9e1c-295f181cdb48/app_logo_final_1769840116895.png"
with open("debug_output.txt", "w") as f:
    f.write(f"Checking if src exists: {os.path.exists(src_path)}\n")
    try:
        contents = os.listdir("/Users/hyunsikhwang/.gemini/antigravity/brain/4f8d9901-450d-4f63-9e1c-295f181cdb48/")
        f.write(f"Brain dir contents: {contents}\n")
    except Exception as e:
        f.write(f"Error listing brain dir: {e}\n")
