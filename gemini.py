import requests
import time

# PUT YOUR API KEY HERE
API_KEY = "AIzaSyAIqZQCpGCp9LMCUPK471IAeAuIHsfiyfU"

print("=" * 80)
print("GEMINI API TESTER - COMPREHENSIVE VERSION")
print("=" * 80)

# Test 1: List available models
print("\n📋 STEP 1: Checking what models are available...\n")

list_url = f'https://generativelanguage.googleapis.com/v1beta/models?key={API_KEY}'

try:
    response = requests.get(list_url, timeout=15)
    print(f"List Models Status: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        print("\n✅ Available models that support generateContent:")
        available_models = []
        for model in data.get('models', []):
            if 'generateContent' in model.get('supportedGenerationMethods', []):
                model_name = model.get('name', '').replace('models/', '')
                available_models.append(model_name)
                print(f"  • {model_name}")
        
        if not available_models:
            print("  ⚠️  No models found that support generateContent")
    else:
        print(f"❌ Failed to list models")
        print(f"Response: {response.text[:300]}")
        available_models = [
            'gemini-2.0-flash-exp',
            'gemini-1.5-flash',
            'gemini-1.5-pro'
        ]
        print(f"\n⚠️  Using default model list instead")
        
except Exception as e:
    print(f"❌ Error listing models: {e}")
    available_models = [
        'gemini-2.0-flash-exp',
        'gemini-1.5-flash',
        'gemini-1.5-pro'
    ]

# Test 2: Try to generate content with each model
print("\n" + "=" * 80)
print("📝 STEP 2: Testing content generation...\n")

working_model = None

for model in available_models:
    print(f"\n🔍 Testing: {model}")
    print("-" * 60)
    
    # Build URL correctly (NO "models/" prefix in the model name)
    url = f'https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={API_KEY}'
    
    payload = {
        'contents': [{
            'parts': [{
                'text': 'Say "Hello" in one word only'
            }]
        }]
    }
    
    try:
        response = requests.post(url, json=payload, timeout=15)
        
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            print("✅ SUCCESS!")
            result = response.json()
            
            if 'candidates' in result:
                text = result['candidates'][0]['content']['parts'][0]['text']
                print(f"Response: '{text}'")
                working_model = model
                print(f"\n🎉🎉🎉 FOUND WORKING MODEL: {model} 🎉🎉🎉")
                break
            else:
                print(f"⚠️  Got 200 but unexpected response format: {result}")
                
        elif response.status_code == 400:
            error_msg = response.json().get('error', {}).get('message', 'Unknown')
            print(f"❌ Bad Request: {error_msg}")
            
        elif response.status_code == 403:
            print("❌ Forbidden - API key is invalid or doesn't have permissions")
            print("   Get a new key: https://aistudio.google.com/apikey")
            
        elif response.status_code == 404:
            print("❌ Not Found - Model doesn't exist or isn't available for your key")
            
        elif response.status_code == 429:
            print("⚠️  Rate Limit - Too many requests. Wait 60 seconds...")
            time.sleep(60)
            
        else:
            print(f"❌ Error {response.status_code}")
            try:
                error_data = response.json()
                print(f"   Message: {error_data.get('error', {}).get('message', 'No details')}")
            except:
                print(f"   Raw: {response.text[:200]}")
                
    except requests.exceptions.Timeout:
        print("❌ Request timed out")
        
    except Exception as e:
        print(f"❌ Exception: {type(e).__name__}: {str(e)}")
    
    # Small delay between requests
    time.sleep(2)

# Final results
print("\n" + "=" * 80)
print("RESULTS")
print("=" * 80)

if working_model:
    print(f"\n✅ SUCCESS! Working model: {working_model}")
    print("\n📝 Update your app_integrated.py:")
    print("-" * 80)
    print(f"""
Line 28 (API Key):
GEMINI_API_KEY = "{API_KEY}"

Lines 276-280 (Model configuration):
model_names = [
    '{working_model}'  # ← This model works!
]

for model in model_names:
    url = f'https://generativelanguage.googleapis.com/v1beta/models/{{model}}:generateContent?key={{api_key}}'
    """)
    
else:
    print("\n❌ NO WORKING MODEL FOUND")
    print("\n🔧 Troubleshooting Steps:")
    print("-" * 80)
    print("""
1. Check API Key:
   • Go to: https://aistudio.google.com/apikey
   • Create a BRAND NEW key
   • Wait 5-10 minutes for it to activate
   • Make sure "Gemini API" is enabled

2. Check Your Region:
   • Gemini API might not be available in your country
   • Try using a VPN (US, UK, or EU)

3. Check Billing:
   • Free tier has limits (15 requests/minute)
   • You might need to enable billing

4. Alternative - Disable Gemini:
   In app_integrated.py line 28:
   GEMINI_API_KEY = ""  # Disabled
   
   Your app will still work without AI explanations!

5. Try the Google SDK Instead:
   pip install google-generativeai
   
   import google.generativeai as genai
   genai.configure(api_key="YOUR_KEY")
   model = genai.GenerativeModel('gemini-1.5-flash')
   response = model.generate_content("Hello")
   print(response.text)
""")