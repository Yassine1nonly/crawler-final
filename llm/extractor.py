import json
import re
from llm.client import call_groq

# Nouveau prompt adaptatif
EXTRACTION_SYSTEM = """Tu es un expert en visualisation de données et en langage Mermaid.js.
Ta mission est d'analyser le texte et de choisir le MEILLEUR type de diagramme pour le représenter.

Choix possibles :
1. FLOWCHART (graph TD) : Pour les processus, étapes, algorithmes.
2. TIMELINE (timeline) : Pour l'histoire, les chronologies, les dates.
3. SEQUENCE (sequenceDiagram) : Pour les interactions entre acteurs.
4. MINDMAP (mindmap) : Pour les concepts hiérarchiques.
5. GRAPH (graph LR) : Pour les relations complexes (Knowledge Graph classique).

RÈGLES :
- Retourne UNIQUEMENT un JSON valide.
- Le champ 'mermaid_code' doit contenir le code brut valide.
"""

EXTRACTION_PROMPT = """
Analyse ce texte et génère le diagramme le plus adapté.

Format JSON attendu :
{{
  "diagram_type": "Flowchart|Timeline|Sequence|Mindmap|Graph",
  "title": "Titre court du diagramme",
  "summary": "Explication brève (1 phrase) du choix",
  "mermaid_code": "Le code mermaid ici..."
}}

TEXTE À ANALYSER :
---
{text}
---

Retourne UNIQUEMENT le JSON :
"""

def extract_knowledge(text: str) -> dict:
    """Extrait une structure de diagramme adaptative avec Groq"""
    
    text = text[:6000]
    prompt = EXTRACTION_PROMPT.format(text=text)
    response = call_groq(prompt, system=EXTRACTION_SYSTEM)
    
    if not response:
        return None
    
    try:
        # Nettoyage standard (comme avant)
        response = response.strip()
        if "```json" in response: response = re.sub(r'```json\s*', '', response)
        if "```" in response: response = re.sub(r'```\s*', '', response)
        
        json_match = re.search(r'\{.*\}', response, re.DOTALL)
        if json_match: response = json_match.group(0)
        
        data = json.loads(response)
        
        # Validation basique
        if "mermaid_code" not in data:
            raise ValueError("Pas de code Mermaid généré")
            
        print(f"   🧠 Type détecté: {data.get('diagram_type')} - {data.get('title')}")
        return data
        
    except Exception as e:
        print(f"   ❌ Erreur extraction: {e}")
        return None