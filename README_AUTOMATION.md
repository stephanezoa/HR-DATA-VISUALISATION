# Automatisation Excel -> PDF

## Script

Le script principal est `generate_arrets_reports.py`.

Il lit un classeur du meme type que `ANALYSE ARRETS MES Modif 14082025.xlsx`, s'appuie sur la feuille `Base` et genere :

- une page de synthese par groupe `chaine + nature`
- des dashboards equipement par equipement
- des PDF soit par groupe, soit en un seul document complet

## Lancer le script

Activer l'environnement :

```bash
source .venv/bin/activate
```

Exemples :

```bash
python generate_arrets_reports.py "ANALYSE ARRETS MES Modif 14082025.xlsx"
python generate_arrets_reports.py "ANALYSE ARRETS MES Modif 14082025.xlsx" --mode combined
python generate_arrets_reports.py "ANALYSE ARRETS MES Modif 14082025.xlsx" --mode both
python generate_arrets_reports.py "ANALYSE ARRETS MES Modif 14082025.xlsx" --chains CH2 CH5 --natures MECA
python generate_arrets_reports.py "ANALYSE ARRETS MES Modif 14082025.xlsx" --chains CH2 --natures ELEC --equipments DATEUSE "CONTROLEUR DE CAISSES"
```

## Options utiles

- `--output-dir exports` : dossier de sortie
- `--year 2025` : annee a exporter
- `--mode grouped|combined|both` : un PDF par groupe, un PDF global, ou les deux
- `--chains CH2 CH5` : filtrer certaines chaines
- `--natures ELEC EXPL MECA` : filtrer certaines natures
- `--equipments DATEUSE` : limiter a certains equipements
- `--skip-overview` : supprimer la page de synthese multi-machines

## Sortie

Par defaut, les PDF sont ecrits dans le dossier `exports/`.

## Version Flask

Une interface web est aussi disponible dans `app.py`.

Lancer le serveur :

```bash
source .venv/bin/activate
flask --app app run --debug
```

Puis ouvrir `http://127.0.0.1:5000`.

Workflow web :

- upload du classeur Excel
- inspection du fichier, de sa version Excel et des feuilles detectees
- choix des chaines, natures, annee et mode d'export
- telechargement d'un PDF ou d'un ZIP classe par groupe
