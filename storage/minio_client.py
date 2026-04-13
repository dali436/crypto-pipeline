"""
Stockage - Client MinIO (Data Lake)
Sauvegarde les fichiers JSON bruts dans des buckets MinIO
organisés par type de données et par date.

"""

import json
import io
from datetime import datetime
from minio import Minio
from minio.error import S3Error

# ── Configuration ──────────────────────────────────────────────
MINIO_ENDPOINT  = "localhost:9000"
MINIO_ACCESS    = "minioadmin"
MINIO_SECRET    = "minioadmin"
MINIO_SECURE    = False             # False = HTTP local, True = HTTPS

# Noms des buckets (= dossiers principaux dans MinIO)
BUCKET_PRICES     = "raw-prices"
BUCKET_HISTORICAL = "raw-historical"
BUCKET_STREAMING  = "raw-streaming"

# ── Initialisation du client MinIO ─────────────────────────────
client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS,
    secret_key=MINIO_SECRET,
    secure=MINIO_SECURE,
)


# ── Créer les buckets s'ils n'existent pas ─────────────────────
def create_buckets():
    """
    Crée les buckets MinIO au premier lancement.
    Sans ça, la sauvegarde échoue car le bucket n'existe pas.
    """
    for bucket in [BUCKET_PRICES, BUCKET_HISTORICAL, BUCKET_STREAMING]:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
            print(f"[OK] Bucket créé : {bucket}")
        else:
            print(f"[--] Bucket déjà existant : {bucket}")


# ── Sauvegarder un dict Python en JSON dans MinIO ──────────────
def save_json_to_minio(data: dict, bucket: str, object_name: str) -> bool:
    """
    Convertit un dict en JSON et l'upload dans un bucket MinIO.

    Args:
        data        : le dictionnaire Python à sauvegarder
        bucket      : nom du bucket cible (ex: "raw-prices")
        object_name : chemin du fichier dans le bucket
                      (ex: "2024/01/15/prices_143000.json")

    Returns:
        True si succès, False si erreur
    """
    try:
        json_bytes = json.dumps(data, indent=2, ensure_ascii=False).encode("utf-8")
        json_stream = io.BytesIO(json_bytes)

        client.put_object(
            bucket_name=bucket,
            object_name=object_name,
            data=json_stream,
            length=len(json_bytes),
            content_type="application/json",
        )
        print(f"[OK] MinIO ← {bucket}/{object_name}")
        return True

    except S3Error as e:
        print(f"[ERREUR] MinIO : {e}")
        return False


# ── Générer un chemin de fichier organisé par date ─────────────
def make_object_name(prefix: str, coin: str = "") -> str:
    """
    Génère un nom de fichier structuré par date pour MinIO.
    Ex: "prices/2024/01/15/bitcoin_143022.json"

    L'organisation par date permet de retrouver facilement
    les données d'un jour précis (partitionnement temporel).
    """
    now = datetime.utcnow()
    date_path = now.strftime("%Y/%m/%d")
    timestamp = now.strftime("%H%M%S")

    if coin:
        return f"{prefix}/{date_path}/{coin}_{timestamp}.json"
    return f"{prefix}/{date_path}/{prefix}_{timestamp}.json"


# ── Fonctions de sauvegarde spécialisées ──────────────────────
def save_current_prices(data: dict):
    """Sauvegarde les prix actuels dans le bucket raw-prices."""
    object_name = make_object_name("prices")
    return save_json_to_minio(data, BUCKET_PRICES, object_name)


def save_historical(data: dict, coin: str):
    """Sauvegarde l'historique d'une crypto dans raw-historical."""
    object_name = make_object_name("historical", coin)
    return save_json_to_minio(data, BUCKET_HISTORICAL, object_name)


def save_kafka_tick(tick: dict):
    """Sauvegarde un tick Kafka dans raw-streaming."""
    object_name = make_object_name("ticks", tick.get("coin", "unknown"))
    return save_json_to_minio(tick, BUCKET_STREAMING, object_name)


# ── Lister les fichiers dans un bucket ────────────────────────
def list_files(bucket: str, prefix: str = "") -> list:
    """
    Liste les fichiers présents dans un bucket MinIO.
    Utile pour vérifier que les données sont bien sauvegardées.
    """
    try:
        objects = client.list_objects(bucket, prefix=prefix, recursive=True)
        files = []
        for obj in objects:
            files.append({
                "name":          obj.object_name,
                "size_kb":       round(obj.size / 1024, 2),
                "last_modified": obj.last_modified.isoformat(),
            })
        return files
    except S3Error as e:
        print(f"[ERREUR] Listage MinIO : {e}")
        return []


# ── Test rapide ────────────────────────────────────────────────
if __name__ == "__main__":
    print("=== Test du client MinIO ===\n")

    # 1. Créer les buckets
    create_buckets()

    # 2. Sauvegarder un fichier de test
    test_data = {
        "test":        True,
        "message":     "Connexion MinIO OK",
        "timestamp":   datetime.utcnow().isoformat(),
    }
    save_json_to_minio(test_data, BUCKET_PRICES, "test/connexion_test.json")

    # 3. Lister les fichiers
    print("\nFichiers dans raw-prices :")
    for f in list_files(BUCKET_PRICES):
        print(f"  - {f['name']} ({f['size_kb']} KB)")

    print("\nMinIO accessible sur : http://localhost:9001")
    print("Login : minioadmin / minioadmin")
