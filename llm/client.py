from openai import OpenAI
from config.settings import GROQ_API_KEY, GROQ_BASE_URL, MODEL, MAX_TOKENS, TEMPERATURE

LAST_ERROR = ""

# Verifier la cle
if not GROQ_API_KEY:
    raise ValueError(
        "ERROR: GROQ_API_KEY manquante.\n"
        "Ajoutez-la dans le fichier .env:\n"
        "GROQ_API_KEY=gsk_..."
    )

# Initialiser le client Groq via OpenAI SDK
try:
    client = OpenAI(
        api_key=GROQ_API_KEY,
        base_url=GROQ_BASE_URL
    )
    print(f"Groq ({MODEL}) initialise")
except Exception as e:
    print(f"Erreur initialisation Groq: {e}")
    raise


def call_groq(prompt: str, system: str = "") -> str:
    """Appelle Groq API via OpenAI SDK"""
    global LAST_ERROR
    LAST_ERROR = ""
    try:
        messages = []

        # Ajouter le message systeme si fourni
        if system:
            messages.append({"role": "system", "content": system})

        # Ajouter le prompt utilisateur
        messages.append({"role": "user", "content": prompt})

        # Appel API
        response = client.chat.completions.create(
            model=MODEL,
            messages=messages,
            max_tokens=MAX_TOKENS,
            temperature=TEMPERATURE,
        )

        return response.choices[0].message.content

    except Exception as e:
        LAST_ERROR = f"LLM error: {e}"
        print(f"Erreur appel Groq: {e}")

        # Messages d'aide selon le type d'erreur
        error_str = str(e).lower()
        if "authentication" in error_str or "api_key" in error_str:
            print("Verifiez votre GROQ_API_KEY dans .env")
        elif "rate_limit" in error_str:
            print("Trop de requetes, attendez quelques secondes")
        elif "model" in error_str:
            print(f"Modele '{MODEL}' non disponible")
            print("Modeles disponibles : llama-3.3-70b-versatile, mixtral-8x7b-32768")
        elif "connection" in error_str:
            print("Verifiez votre connexion internet")

        return ""

# Alias pour compatibilite
call_claude = call_groq
call_gemini = call_groq
call_grok = call_groq
