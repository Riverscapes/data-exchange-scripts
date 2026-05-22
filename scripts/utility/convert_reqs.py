import os

with open('requirements.txt', encoding='utf8') as f:
    for line in f:
        package = line.strip()
        os.system(f"uv add {package}")

        # uv add -r requirements.txt
