import urllib.request, urllib.error, json, time

def fetch(url):
    return urllib.request.urlopen(url, timeout=5).read().decode()

urls = [
    'http://127.0.0.1:8000/observations?maladie=cancer&indicateur=prevalence&annee=2018',
    'http://127.0.0.1:8000/stats?maladie=cancer&indicateur=prevalence'
]

for _ in range(20):
    try:
        results = [fetch(u) for u in urls]
        print('OBSERVATIONS (truncated):')
        print(results[0][:1000])
        print('\nSTATS:')
        print(results[1])
        break
    except Exception as e:
        time.sleep(0.5)
else:
    print('Échec : impossible de joindre l\'API sur 127.0.0.1:8000')
