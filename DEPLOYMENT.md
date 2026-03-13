# Déploiement HR Data Visualization

## Installation automatique

Pour déployer l'application sur **data-visual.perenkap-api.online**, exécutez simplement :

```bash
cd /home/perenkap/HR-DATA-VISUALISATION
./install.sh
```

Le script va :
1. ✅ Installer les dépendances Python (pandas, Flask, matplotlib, etc.)
2. ✅ Installer Gunicorn pour servir l'application
3. ✅ Créer les répertoires nécessaires (logs, jobs, archive)
4. ✅ Configurer le service systemd
5. ✅ Configurer Nginx avec SSL
6. ✅ Démarrer l'application

## Architecture du déploiement

```
Internet (HTTPS)
      ↓
Nginx (port 443) - SSL terminaison
      ↓
Gunicorn (127.0.0.1:5001) - 4 workers
      ↓
Flask App (HR Data Visualization)
```

## Fichiers de configuration

- **data-visual.service** : Service systemd pour gérer l'application
- **data-visual.nginx.conf** : Configuration Nginx pour le reverse proxy et SSL
- **install.sh** : Script d'installation automatique

## Commandes utiles

### Gestion du service

```bash
# Démarrer
sudo systemctl start data-visual.service

# Arrêter
sudo systemctl stop data-visual.service

# Redémarrer
sudo systemctl restart data-visual.service

# Statut
sudo systemctl status data-visual.service

# Activer au démarrage
sudo systemctl enable data-visual.service
```

### Logs

```bash
# Logs systemd (en temps réel)
sudo journalctl -u data-visual.service -f

# Logs Gunicorn
tail -f /home/perenkap/HR-DATA-VISUALISATION/logs/error.log
tail -f /home/perenkap/HR-DATA-VISUALISATION/logs/access.log

# Logs système (stdout/stderr)
tail -f /home/perenkap/HR-DATA-VISUALISATION/logs/stdout.log
tail -f /home/perenkap/HR-DATA-VISUALISATION/logs/stderr.log

# Logs Nginx
sudo tail -f /var/log/nginx/data-visual-access.log
sudo tail -f /var/log/nginx/data-visual-error.log
```

### Nginx

```bash
# Tester la configuration
sudo nginx -t

# Recharger la configuration
sudo systemctl reload nginx

# Redémarrer Nginx
sudo systemctl restart nginx
```

## Mise à jour de l'application

```bash
cd /home/perenkap/HR-DATA-VISUALISATION

# Pull les changements depuis Git
git pull

# Activer l'environnement virtuel
source venv/bin/activate

# Installer les nouvelles dépendances si nécessaire
pip install -r requirements.txt

# Redémarrer le service
sudo systemctl restart data-visual.service
```

## Dépannage

### L'application ne démarre pas

1. Vérifier les logs :
   ```bash
   sudo journalctl -u data-visual.service -n 50
   ```

2. Vérifier que Gunicorn est installé :
   ```bash
   source venv/bin/activate
   which gunicorn
   ```

3. Tester l'application manuellement :
   ```bash
   cd /home/perenkap/HR-DATA-VISUALISATION
   source venv/bin/activate
   python app.py
   ```

### Erreur 502 Bad Gateway

- Le service n'est pas démarré : `sudo systemctl start data-visual.service`
- Gunicorn écoute sur le mauvais port : vérifier les logs
- Problème de permissions : vérifier les permissions des fichiers

### Erreur SSL

Vérifier que les certificats existent :
```bash
sudo ls -la /etc/letsencrypt/live/perenkap-api.online/
```

Si nécessaire, obtenir un nouveau certificat :
```bash
sudo certbot certonly --nginx -d data-visual.perenkap-api.online
```

## Structure des répertoires

```
/home/perenkap/HR-DATA-VISUALISATION/
├── app.py                      # Point d'entrée de l'application
├── hr_app/                     # Package Flask principal
├── templates/                  # Templates HTML
├── static/                     # Fichiers statiques (CSS, JS)
├── venv/                       # Environnement virtuel Python
├── logs/                       # Logs de l'application
├── jobs/                       # Sessions d'import temporaires
├── archive/                    # Rapports archivés
├── data-visual.service         # Service systemd
├── data-visual.nginx.conf      # Configuration Nginx
├── install.sh                  # Script d'installation
└── requirements.txt            # Dépendances Python
```

## Sécurité

- ✅ SSL/TLS activé (HTTPS uniquement)
- ✅ Application en écoute locale (127.0.0.1)
- ✅ Reverse proxy Nginx
- ✅ Upload limité à 50 MB
- ✅ Service tournant sous l'utilisateur `perenkap` (non-root)

## Performance

- **4 workers Gunicorn** : ajustable selon la charge
- **Timeout 120s** : pour les rapports longs
- **Cache statique** : 30 jours pour les fichiers CSS/JS
