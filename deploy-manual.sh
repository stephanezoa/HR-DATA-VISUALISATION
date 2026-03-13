#!/bin/bash
# Script de déploiement manuel - exécutez ces commandes une par une

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

PROJECT_DIR="/home/perenkap/HR-DATA-VISUALISATION"

echo -e "${GREEN}=== Déploiement HR Data Visualization ===${NC}\n"

# Partie 1: Sans sudo
echo -e "${GREEN}[1/7] Installation des dépendances Python...${NC}"
cd "$PROJECT_DIR"
if [ ! -d "venv" ]; then
    python3 -m venv venv
fi
source venv/bin/activate
pip install --upgrade pip -q
pip install -r requirements.txt -q
pip install gunicorn -q

echo -e "${GREEN}[2/7] Création des répertoires...${NC}"
mkdir -p logs jobs archive
chmod 755 logs

echo -e "${GREEN}[3/7] Test de l'application...${NC}"
python -c "from hr_app import create_app; app = create_app(); print('✓ Application chargée')"

echo -e "\n${YELLOW}Maintenant, exécutez les commandes suivantes avec sudo:${NC}\n"

cat << 'COMMANDS'
# [4/7] Installer le service systemd
sudo cp /home/perenkap/HR-DATA-VISUALISATION/data-visual.service /etc/systemd/system/
sudo systemctl daemon-reload

# [5/7] Configurer Nginx
sudo cp /home/perenkap/HR-DATA-VISUALISATION/data-visual.nginx.conf /etc/nginx/sites-available/data-visual
sudo ln -sf /etc/nginx/sites-available/data-visual /etc/nginx/sites-enabled/

# [6/7] Tester Nginx
sudo nginx -t

# [7/7] Démarrer les services
sudo systemctl enable data-visual.service
sudo systemctl start data-visual.service
sudo systemctl reload nginx

# Vérifier le statut
sudo systemctl status data-visual.service
COMMANDS

echo -e "\n${GREEN}Partie 1/2 terminée !${NC}"
echo -e "${YELLOW}Copiez et exécutez les commandes ci-dessus pour terminer l'installation.${NC}"
