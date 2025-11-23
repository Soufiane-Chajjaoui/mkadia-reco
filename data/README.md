# Génération de Données Synthétiques (MKADIA)

Cette suite d'outils génère des données e-commerce réalistes (catégories, produits, utilisateurs, commandes) à l'aide d'un LLM léger (Ollama ou Hugging Face), puis propose une analyse FP-GROWTH et un import dans la base de données.

## Installation rapide

```bash
cd data
chmod +x setup_env.sh
./setup_env.sh        # crée venv + .env + installe les dépendances
source venv/bin/activate
```

Windows PowerShell :
```powershell
cd data
.\setup_env.ps1
.\venv\Scripts\Activate.ps1
```

## Configuration

Le script `setup_env.sh` crée un fichier `.env`. Modifiez-le pour forcer un provider :
```env
LLM_PROVIDER=ollama            # auto, ollama, huggingface
OLLAMA_MODEL=phi3:mini
HF_MODEL=TinyLlama/TinyLlama-1.1B-Chat-v1.0

DB_HOST=localhost
DB_NAME=mkadia_db
DB_USER=root
DB_PASSWORD=
```

## Génération des données

```bash
python generate_data_with_llm.py --mode sample   # petit jeu (2 éléments)
python generate_data_with_llm.py --mode full     # dataset complet
```

Le script sauvegarde `synthetic_data.json` et affiche un aperçu dans le terminal.

## Analyse & import

1. **Analyse FP-GROWTH**
   ```bash
   python fp_growth_recommendations.py 5 0.3
   ```
   Résultat : `fp_growth_results.json` avec motifs fréquents, règles et recommandations.

2. **Import en base**
   ```bash
   python import_to_database.py
   ```
   Utilise `synthetic_data.json` + `fp_growth_results.json` (table `product_recommendations` incluse).

3. **Workflow complet**
   ```bash
   python workflow_complete.py
   ```

## Scripts disponibles

| Fichier | Description |
| --- | --- |
| `generate_data_with_llm.py` | Génération de données synthétiques via LLM |
| `fp_growth_recommendations.py` | Découverte de motifs & recommandations |
| `import_to_database.py` | Import MySQL (catégories, produits, utilisateurs, commandes, reco) |
| `workflow_complete.py` | Orchestration complète (génération → analyse → import) |
| `example_api_usage.py` | Exemples d'utilisation des recommandations côté Spring Boot / Python |
| `schema_recommendations.sql` | Table et vue `product_recommendations` |
| `test_setup.py` | Vérifie dépendances + Ollama + Hugging Face |
| `setup_env.sh` / `setup_env.ps1` | Création/activation de l'environnement local |
| `GUIDE_HUGGINGFACE.md` | Guide dédié au mode Hugging Face |
| `QUICKSTART.md` | Récapitulatif express |

## Modèles recommandés

### Ollama
- `phi3:mini` (≈2.2 GB) – recommandé
- `llama3.2:1b`
- `gemma:2b`

### Hugging Face
- `TinyLlama/TinyLlama-1.1B-Chat-v1.0`
- `Qwen/Qwen2-0.5B-Instruct`
- `microsoft/Phi-3-mini-4k-instruct`

## Problèmes fréquents

| Problème | Solution |
| --- | --- |
| `curl: no URL specified` | Utiliser la commande complète d’installation Ollama |
| `pull model manifest: file does not exist` | `ollama list` puis `ollama pull <nom exact>` |
| `Aucun LLM détecté` | Vérifier `.env`, `ollama serve`, installations HF |
| `CUDA out of memory` | Utiliser un plus petit modèle ou `device_map="cpu"` |

## Vérification

```bash
python test_setup.py
```
Affiche le statut des dépendances, d’Ollama et d’Hugging Face.

