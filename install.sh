#!/bin/bash
set -e

echo "=== Installation de HR Data Visualization ==="

# Couleurs pour l'affichage
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

PROJECT_DIR="/home/perenkap/HR-DATA-VISUALISATION"

# Vérifier que le script est exécuté depuis le bon répertoire
if [ ! -f "$PROJECT_DIR/app.py" ]; then
    echo -e "${RED}Erreur: app.py non trouvé dans $PROJECT_DIR${NC}"
    exit 1
fi

# 1. Installer les dépendances Python
echo -e "${GREEN}[1/7] Installation des dépendances Python...${NC}"
cd "$PROJECT_DIR"

# Créer le venv si nécessaire
if [ ! -d "venv" ]; then
    echo "Création de l'environnement virtuel..."
    python3 -m venv venv
fi

# Activer et installer les dépendances
source venv/bin/activate
pip install --upgrade pip -q
pip install -r requirements.txt -q
pip install gunicorn -q

# 2. Créer les répertoires nécessaires
echo -e "${GREEN}[2/7] Création des répertoires...${NC}"
mkdir -p "$PROJECT_DIR/logs"
mkdir -p "$PROJECT_DIR/jobs"
mkdir -p "$PROJECT_DIR/archive"
chmod 755 "$PROJECT_DIR/logs"

# 3. Tester l'application
echo -e "${GREEN}[3/7] Test de l'application Flask...${NC}"
python -c "from hr_app import create_app; app = create_app(); print('✓ Application chargée avec succès')"

# 4. Copier le service systemd
echo -e "${GREEN}[4/7] Configuration du service systemd...${NC}"
sudo cp "$PROJECT_DIR/data-visual.service" /etc/systemd/system/
sudo systemctl daemon-reload

# 5. Copier la configuration nginx
echo -e "${GREEN}[5/7] Configuration de Nginx...${NC}"
sudo cp "$PROJECT_DIR/data-visual.nginx.conf" /etc/nginx/sites-available/data-visual
sudo ln -sf /etc/nginx/sites-available/data-visual /etc/nginx/sites-enabled/

# 6. Tester la configuration nginx
echo -e "${GREEN}[6/7] Test de la configuration Nginx...${NC}"
sudo nginx -t

# 7. Activer et démarrer les services
echo -e "${GREEN}[7/7] Activation et démarrage des services...${NC}"
sudo systemctl enable data-visual.service
sudo systemctl restart data-visual.service
sudo systemctl reload nginx

# Attendre que le service démarre
sleep 2

# Vérifier le statut
echo -e "\n${GREEN}=== Statut du service ===${NC}"
sudo systemctl status data-visual.service --no-pager -l

echo -e "\n${GREEN}=== Installation terminée avec succès ===${NC}"
echo -e "Application disponible sur: ${YELLOW}https://data-visual.perenkap-api.online${NC}"
echo -e "\n${YELLOW}Commandes utiles:${NC}"
echo -e "  ${GREEN}Logs en temps réel:${NC}  sudo journalctl -u data-visual.service -f"
echo -e "  ${GREEN}Redémarrer:${NC}         sudo systemctl restart data-visual.service"
echo -e "  ${GREEN}Arrêter:${NC}            sudo systemctl stop data-visual.service"
echo -e "  ${GREEN}Statut:${NC}             sudo systemctl status data-visual.service"
echo -e "  ${GREEN}Logs d'erreur:${NC}      tail -f $PROJECT_DIR/logs/error.log"
echo -e "  ${GREEN}Logs d'accès:${NC}       tail -f $PROJECT_DIR/logs/access.log"
