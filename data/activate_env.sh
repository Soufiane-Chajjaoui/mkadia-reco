#!/bin/bash
# Script pour activer l'environnement virtuel

if [ -d "venv" ]; then
    source venv/bin/activate
    echo "✅ Environnement virtuel activé"
    echo "💡 Pour désactiver: deactivate"
else
    echo "❌ Environnement virtuel non trouvé"
    echo "💡 Exécutez d'abord: ./setup_env.sh"
fi
