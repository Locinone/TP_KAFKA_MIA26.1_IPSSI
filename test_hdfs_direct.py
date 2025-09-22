#!/usr/bin/env python3
"""
Test direct d'écriture dans HDFS sans Spark
"""
import os
import json
import time
from hdfs import InsecureClient

def test_hdfs_direct():
    """Test d'écriture directe dans HDFS"""
    try:
        # Connexion à HDFS
        client = InsecureClient('http://localhost:9870', user='root')
        
        # Créer un répertoire de test
        test_dir = '/weather-data/test'
        if not client.status(test_dir, strict=False):
            client.makedirs(test_dir)
            print(f"✅ Répertoire créé: {test_dir}")
        
        # Créer un fichier de test
        test_data = {
            "city": "Paris",
            "country": "France",
            "region": "Europe",
            "continent": "Europe",
            "timestamp": int(time.time()),
            "date": time.strftime("%Y-%m-%d"),
            "hour": time.strftime("%H"),
            "weather": {
                "temperature": 20.5,
                "windspeed": 15.2,
                "winddirection": 180,
                "weathercode": 1,
                "is_day": 1,
                "time": time.strftime("%Y-%m-%dT%H:%M")
            }
        }
        
        # Écrire le fichier
        test_file = f"{test_dir}/test_data_{int(time.time())}.json"
        with client.write(test_file) as writer:
            writer.write(json.dumps(test_data, indent=2).encode('utf-8'))
        
        print(f"✅ Fichier écrit: {test_file}")
        
        # Lister le contenu
        files = client.list(test_dir)
        print(f"📁 Fichiers dans {test_dir}: {files}")
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur: {e}")
        return False

if __name__ == "__main__":
    print("🧪 Test d'écriture directe dans HDFS")
    print("=" * 40)
    
    success = test_hdfs_direct()
    
    if success:
        print("✅ Test réussi!")
    else:
        print("❌ Test échoué!")
