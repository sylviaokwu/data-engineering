#!/bin/bash

# Create necessary directories
echo "Creating directories..."
mkdir -p ./secrets ./dags ./logs ./plugins

# Set permissions for Airflow (1000 is the default Airflow UID)
echo "Setting permissions..."
chmod -R 775 ./dags ./logs ./plugins

# Create .env file if it doesn't exist
if [ ! -f .env ]; then
    echo "Creating .env from .env.example..."
    cp .env.example .env
    
    # Generate Fernet Key
    echo "Generating Airflow Fernet Key..."
    FERNET_KEY=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())" 2>/dev/null || \
                 python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())" 2>/dev/null || \
                 echo "fallback_key_please_replace_me")
    
    # Replace placeholder in .env
    if [[ "$OSTYPE" == "darwin"* ]]; then
        sed -i '' "s|GENERATED_FERNET_KEY|\"$FERNET_KEY\"|" .env
        sed -i '' "s|AIRFLOW_UID=1000|AIRFLOW_UID=$(id -u)|" .env
    else
        sed -i "s|GENERATED_FERNET_KEY|\"$FERNET_KEY\"|" .env
        sed -i "s|AIRFLOW_UID=1000|AIRFLOW_UID=$(id -u)|" .env
    fi
    
    echo ".env file created. Please update it with your GCP credentials."
else
    echo ".env already exists. Skipping creation."
fi

echo "Setup complete!"
echo "Next steps:"
echo "1. Put your GCP service account key in ./secrets/gcp-key.json"
echo "2. Update .env with your GCP project details"
echo "3. Run 'docker-compose up'"
