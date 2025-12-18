from fastapi.testclient import TestClient
import api

client = TestClient(api.app)

print('Interroge /observations...')
resp = client.get('/observations', params={'maladie':'cancer','indicateur':'prevalence','annee':2018,'limit':5})
print('status', resp.status_code)
try:
    data = resp.json()
    print('observations sample:', data[:5])
except Exception as e:
    print('Erreur parsing JSON observations:', e, resp.text[:200])

print('\nInterroge /stats...')
resp2 = client.get('/stats', params={'maladie':'cancer','indicateur':'prevalence'})
print('status', resp2.status_code)
try:
    data2 = resp2.json()
    print('stats sample:', data2[:10])
except Exception as e:
    print('Erreur parsing JSON stats:', e, resp2.text[:200])
