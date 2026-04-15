# data-engineering

pip install terraform-binary

export GOOGLE_CREDENTIALS='/workspaces/data-engineering/terraform/keys/gcp-key.json'

echo $GOOGLE_CREDENTIALS to confirm path



# Download directly from HashiCorp
wget https://releases.hashicorp.com/terraform/1.7.0/terraform_1.7.0_linux_amd64.zip
unzip terraform_1.7.0_linux_amd64.zip
sudo mv terraform /usr/local/bin/










docker exec -it de_airflow_scheduler bash