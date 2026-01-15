les sources donnee par utilisateur (HtML, text..)

les key word donnee par utilisateur

j lance le scraping (paarametre : la fréquence (heure/jour) , nombre de page a consulter)  , récupère le contenu , si le contenu est valable avec les word key il stock sinon il stock pas en NOSQL

Analyser les donnees stockee par des Dashboard ,graphe.... generee par LLM (les graphes doit etre cree en se basant en s adaptant avec les donnees stockee)

Interface web locale:
- Installer les dependances: `pip install -r requirements.txt`
- Lancer le serveur: `python server/app.py`
- Ouvrir `http://localhost:8000`

Notes:
- MongoDB doit etre demarre pour le crawling et le stockage.
- Filtrage par mots-cles: renseignez des keywords (ex: finance, education) pour ne stocker que le contenu pertinent.
- Pour les sites difficiles (Cloudflare/JS): installez Playwright et ses navigateurs `pip install playwright` puis `playwright install`. Selenium est aussi supporte si Chrome est installe.
