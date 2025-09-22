#!/usr/bin/env python3
"""
Script de test pour vérifier l'API Open-Meteo
"""

import requests
import json

def test_weather_api(latitude, longitude):
    """
    Teste l'API Open-Meteo avec les coordonnées données
    """
    try:
        url = f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&current_weather=true"
        print(f"🌤️  Testing API: {url}")
        
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        print("✅ API Response received successfully!")
        print(f"📊 Raw response: {json.dumps(data, indent=2)}")
        
        # Vérifier la structure des données
        if 'current_weather' in data:
            weather = data['current_weather']
            print(f"\n🌡️  Temperature: {weather.get('temperature')}°C")
            print(f"💨 Wind Speed: {weather.get('windspeed')} km/h")
            print(f"🧭 Wind Direction: {weather.get('winddirection')}°")
            print(f"☁️  Weather Code: {weather.get('weathercode')}")
            print(f"🌅 Is Day: {weather.get('is_day')}")
            print(f"⏰ Time: {weather.get('time')}")
        else:
            print("❌ No current_weather data found in response")
            
        return True
        
    except Exception as e:
        print(f"❌ Error testing API: {e}")
        return False

if __name__ == "__main__":
    # Test avec Paris
    print("🧪 Testing with Paris coordinates (48.8566, 2.3522)")
    test_weather_api(48.8566, 2.3522)
    
    print("\n" + "="*50 + "\n")
    
    # Test avec New York
    print("🧪 Testing with New York coordinates (40.7128, -74.0060)")
    test_weather_api(40.7128, -74.0060)
