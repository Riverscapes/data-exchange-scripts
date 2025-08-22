input_path = r"C:\nardata\work\rme_extraction\20250820-yct\yct.csv"
output_path = r"C:\nardata\work\rme_extraction\20250820-yct\yct19.csv"
numlines=19
# includes header, if any

with open(input_path, 'r', encoding='utf-8') as infile, open(output_path, 'w', encoding='utf-8') as outfile:
    for i, line in enumerate(infile):
        if i < numlines:
            outfile.write(line)
        else:
            break
print(f"Done! First {numlines} lines written to {output_path}.")