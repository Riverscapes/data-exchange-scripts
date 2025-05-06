import subprocess

# cmd = "ogr2ogr --version"

cmd = "/opt/homebrew/bin/ogr2ogr -append -nln project_bounds /Users/philipbailey/GISData/riverscapes/rme-scrape/rme_scrape.gpkg /Users/philipbailey/GISData/riverscapes/rme-scrape/downloads/1804001201/project_bounds.geojson"
output = subprocess.run(cmd, shell=True, check=True, capture_output=True)
print("OUTPUT:", output.stdout.decode("utf-8").strip())
