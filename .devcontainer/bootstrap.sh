#!/bin/bash

wget https://raw.githubusercontent.com/Riverscapes/environment/master/nar-ys.zsh-theme -O ~/.oh-my-zsh/custom/themes/nar-ys.zsh-theme
wget https://raw.githubusercontent.com/Riverscapes/environment/master/.aliases -O ~/.aliases
wget https://raw.githubusercontent.com/Riverscapes/environment/master/.zshrc -O ~/.zshrc

uv sync

# Install system GDAL and the matching Python gdal package.
# The Python gdal version must match the system libgdal version, so it cannot
# be pinned in pyproject.toml generically. We install it here explicitly.
sudo apt-get update
sudo apt-get install -y --no-install-recommends libgdal-dev gdal-bin libsqlite3-mod-spatialite unzip
bash install_geo.sh

# Install AWS CLI v2
curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "/tmp/awscliv2.zip"
unzip -q /tmp/awscliv2.zip -d /tmp
sudo /tmp/aws/install
rm -rf /tmp/awscliv2.zip /tmp/aws