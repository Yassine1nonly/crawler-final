import requests

def visualize_graph(knowledge_data: dict, output_file: str = "output_graph.svg"):
    """
    Génère une image vectorielle (SVG) via l'API Kroki.
    Cette méthode est robuste et accepte les très gros diagrammes.
    """
    mermaid_code = knowledge_data.get('mermaid_code')
    diagram_type = knowledge_data.get('diagram_type', 'Unknown')
    
    if not mermaid_code:
        print("⚠️ Pas de code Mermaid à visualiser")
        return

    print(f"🎨 Génération du diagramme ({diagram_type}) via Kroki...")

    try:
        # On utilise l'API Kroki en POST (pas de limite de taille)
        url = "https://kroki.io/mermaid/svg"
        
        # Le code est envoyé dans le corps de la requête
        response = requests.post(url, data=mermaid_code.encode('utf-8'))
        
        if response.status_code == 200:
            # Sauvegarde du fichier
            with open(output_file, 'wb') as f:
                f.write(response.content)
            print(f"✅ Image sauvegardée : {output_file}")
        
        else:
            print(f"❌ Erreur API Kroki ({response.status_code})")
            # Affiche le début du code pour débugger si besoin
            print(f"   Code envoyé (début) : {mermaid_code[:50]}...")
            
    except Exception as e:
        print(f"❌ Erreur système : {e}")