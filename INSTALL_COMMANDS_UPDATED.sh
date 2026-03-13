#!/bin/bash
# Commandes d'installation mises à jour pour votre serveur

set -e

echo "═══════════════════════════════════════════════════════════════"
echo "  INSTALLATION HR DATA VISUALIZATION - Version corrigée"
echo "═══════════════════════════════════════════════════════════════"
echo ""

# 1. Installer le service systemd
echo "📦 [1/5] Installation du service systemd..."
sudo cp /home/perenkap/HR-DATA-VISUALISATION/data-visual.service /etc/systemd/system/
sudo systemctl daemon-reload
echo "✅ Service systemd installé"
echo ""

# 2. Créer les répertoires nginx si nécessaire
echo "📁 [2/5] Préparation de la configuration Nginx..."
sudo mkdir -p /etc/nginx/conf.d
sudo mkdir -p /etc/nginx/sites-available
sudo mkdir -p /etc/nginx/sites-enabled
echo "✅ Répertoires créés"
echo ""

# 3. Copier la configuration nginx
echo "⚙️  [3/5] Installation de la configuration Nginx..."
if [ -d "/etc/nginx/conf.d" ]; then
    echo "   Utilisation de /etc/nginx/conf.d/"
    sudo cp /home/perenkap/HR-DATA-VISUALISATION/data-visual.nginx.conf /etc/nginx/conf.d/data-visual.conf
elif [ -d "/etc/nginx/sites-available" ]; then
    echo "   Utilisation de /etc/nginx/sites-available/"
    sudo cp /home/perenkap/HR-DATA-VISUALISATION/data-visual.nginx.conf /etc/nginx/sites-available/data-visual
    sudo ln -sf /etc/nginx/sites-available/data-visual /etc/nginx/sites-enabled/
else
    echo "   ⚠️  Structure Nginx non standard détectée"
    echo "   Copie dans /etc/nginx/conf.d/ (création du répertoire)"
    sudo mkdir -p /etc/nginx/conf.d
    sudo cp /home/perenkap/HR-DATA-VISUALISATION/data-visual.nginx.conf /etc/nginx/conf.d/data-visual.conf
fi
echo "✅ Configuration Nginx installée"
echo ""

# 4. Tester la configuration nginx
echo "🧪 [4/5] Test de la configuration Nginx..."
if sudo nginx -t; then
    echo "✅ Configuration Nginx valide"
else
    echo "❌ Erreur dans la configuration Nginx"
    echo "   Vérifiez les logs ci-dessus"
    exit 1
fi
echo ""

# 5. Démarrer les services
echo "🚀 [5/5] Démarrage des services..."
sudo systemctl enable data-visual.service
sudo systemctl start data-visual.service
sleep 2
sudo systemctl reload nginx
echo "✅ Services démarrés"
echo ""

# Vérification
echo "═══════════════════════════════════════════════════════════════"
echo "  VÉRIFICATION DU DÉPLOIEMENT"
echo "═══════════════════════════════════════════════════════════════"
echo ""
sudo systemctl status data-visual.service --no-pager -l
echo ""

# Test local
echo "🧪 Test de l'application en local..."
sleep 1
if curl -s http://127.0.0.1:5001 > /dev/null; then
    echo "✅ Application répond sur le port 5001"
else
    echo "⚠️  Application ne répond pas encore (patientez quelques secondes)"
fi
echo ""

echo "═══════════════════════════════════════════════════════════════"
echo "  ✅ INSTALLATION TERMINÉE !"
echo "═══════════════════════════════════════════════════════════════"
echo ""
echo "🌐 URL: https://data-visual.perenkap-api.online"
echo ""
echo "📋 Commandes utiles:"
echo "   • Logs en temps réel:  sudo journalctl -u data-visual.service -f"
echo "   • Redémarrer:          sudo systemctl restart data-visual.service"
echo "   • Statut:              sudo systemctl status data-visual.service"
echo "   • Logs d'erreur:       tail -f ~/HR-DATA-VISUALISATION/logs/error.log"
echo ""
