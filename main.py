import os
from datetime import datetime
from crawler.web_crawler import WebCrawler
from preprocessing.cleaner import clean_text, truncate_text
from llm.extractor import extract_knowledge
from visualization.plotter import visualize_graph

def pipeline(url: str, max_pages: int = 5):
    print("\n" + "="*60)
    print("🚀 GRAPHCRAWLER - Mode Multi-Diagrammes")
    print("="*60)
    
    # Création dossier
    output_dir = "outputs"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 1. Crawling
    print(f"🔍 Crawling de : {url}")
    crawler = WebCrawler()
    try:
        data = crawler.crawl_url(url, content_types=['html'], max_hits=max_pages)
    except Exception as e:
        print(f"❌ Erreur : {e}")
        return
    
    if not data:
        print("❌ Aucune page trouvée")
        return

    print(f"✅ {len(data)} pages trouvées. Lancement de la génération en série...\n")

    # 2. BOUCLE SUR CHAQUE PAGE (C'est ici que ça change)
    for i, item in enumerate(data, 1):
        title = item.get('title', 'Sans titre')
        content = item.get('content', '')
        
        # Nettoyage
        cleaned_text = clean_text(content)
        
        # Filtre : On ignore les pages trop vides (< 500 caractères)
        if len(cleaned_text) < 500:
            print(f"⏩ Page {i}/{len(data)} ignorée (trop courte) : {title[:30]}...")
            continue

        print(f"⏳ [{i}/{len(data)}] Analyse de : {title[:50]}...")
        
        # Analyse LLM
        text_input = truncate_text(cleaned_text, max_chars=6000)
        knowledge = extract_knowledge(text_input)
        
        if knowledge:
            # Création d'un nom de fichier unique avec index
            safe_title = "".join([c for c in title if c.isalnum() or c in (' ', '-', '_')]).strip()
            safe_title = safe_title.replace(" ", "_")[:20]
            timestamp = datetime.now().strftime("%H%M%S")
            
            # Nom : 1_Titre_Heure.svg
            filename = f"{i}_{safe_title}_{timestamp}.svg"
            output_path = os.path.join(output_dir, filename)
            
            # Génération Image
            visualize_graph(knowledge, output_path)
            print(f"   ✅ Diagramme généré : {filename} ({knowledge.get('diagram_type')})")
        else:
            print("   ❌ Pas de diagramme possible pour cette page")
            
    crawler.close()
    print("\n" + "="*60)
    print(f"🎉 Terminé ! Vérifiez le dossier '{output_dir}/'")
if __name__ == "__main__":
    url = input("🌐 URL: ").strip()
    if url:
        if not url.startswith('http'): url = 'https://' + url
        pipeline(url)