import time
import logging
from flask import Flask, request, jsonify
from cachetools import LRUCache, LFUCache

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CacheManager:
    def __init__(self):
        self.cache_lru = LRUCache(maxsize=500)
        self.cache_lfu = LFUCache(maxsize=500)
        self.timestamps = {}
        self.hits_lru = 0
        self.misses_lru = 0
        self.hits_lfu = 0
        self.misses_lfu = 0
        logger.info("Cache Manager inicializado")
    
    def get_from_cache(self, pregunta):
        now = time.time()
        respuesta_cache = None

        # Buscar en LRU
        if pregunta in self.cache_lru and now - self.timestamps.get(pregunta, 0) < 600:
            self.hits_lru += 1
            respuesta_cache = self.cache_lru[pregunta]
        else:
            self.misses_lru += 1
        
        # Buscar en LFU
        if pregunta in self.cache_lfu and now - self.timestamps.get(pregunta, 0) < 600:
            self.hits_lfu += 1
            _ = self.cache_lfu[pregunta]
        else:
            self.misses_lfu += 1
        
        return respuesta_cache
    
    def update_cache(self, pregunta, respuesta):
        now = time.time()
        self.cache_lru[pregunta] = respuesta
        self.cache_lfu[pregunta] = respuesta
        self.timestamps[pregunta] = now
    
    def get_stats(self):
        total_lru = self.hits_lru + self.misses_lru
        hit_rate_lru_pct = (self.hits_lru / total_lru) * 100 if total_lru > 0 else 0
        
        total_lfu = self.hits_lfu + self.misses_lfu
        hit_rate_lfu_pct = (self.hits_lfu / total_lfu) * 100 if total_lfu > 0 else 0
        
        return {
            "lru": {
                "hits": self.hits_lru,
                "misses": self.misses_lru,
                "hit_rate": hit_rate_lru_pct,
                "size": len(self.cache_lru)
            },
            "lfu": {
                "hits": self.hits_lfu,
                "misses": self.misses_lfu,
                "hit_rate": hit_rate_lfu_pct,
                "size": len(self.cache_lfu)
            }
        }

app = Flask(__name__)
cache_manager = CacheManager()

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "healthy", "service": "cache"})

@app.route('/get', methods=['GET'])
def get_from_cache():
    pregunta = request.args.get('pregunta')
    respuesta = cache_manager.get_from_cache(pregunta)
    return jsonify({"encontrado": respuesta is not None, "respuesta": respuesta})

@app.route('/update', methods=['POST'])
def update_cache():
    data = request.get_json()
    cache_manager.update_cache(data['pregunta'], data['respuesta'])
    return jsonify({"success": True})

@app.route('/stats', methods=['GET'])
def get_stats():
    stats = cache_manager.get_stats()
    return jsonify(stats)

@app.route('/')
def index():
    return jsonify({
        "service": "Cache Service", 
        "status": "running",
        "cache_size_lru": len(cache_manager.cache_lru),
        "cache_size_lfu": len(cache_manager.cache_lfu)
    })

if __name__ == '__main__':
    print(" Esperando consultas...")
    app.run(host='0.0.0.0', port=8002, debug=False)