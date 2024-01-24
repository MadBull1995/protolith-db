#!/bin/bash

# Check if the correct number of arguments was provided
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <appuser> <app_dir>"
    exit 1
fi

APP_USER="$1"
APP_DIR="$2"

# Create a dedicated user
echo "Creating a dedicated user for the application..."
sudo sysadminctl -addUser $APP_USER -fullName "ProtolithDB User"

# Create necessary directories and set permissions
echo "Setting up directories..."
sudo mkdir -p $APP_DIR
sudo chown $APP_USER:$APP_USER $APP_DIR
sudo chmod 700 $APP_DIR

echo "Installation and setup completed successfully."
