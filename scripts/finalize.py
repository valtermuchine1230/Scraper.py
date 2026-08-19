import os
import json
import datetime
from huggingface_hub import HfApi


def get_repo_and_api():
    hf_token = os.environ.get("HF_TOKEN")
    api = HfApi()
    repo_owner = None
    try:
        who = api.whoami(token=hf_token)
        repo_owner = who.get("name") or who.get("user")
    except Exception:
        pass
    return api, repo_owner, hf_token


def upload_state_and_checkpoints():
    save_path = os.environ.get("SAVE_PATH")
    api, repo_owner, hf_token = get_repo_and_api()
    if not hf_token or not save_path:
        return
    checkpoint_repo = f"{repo_owner}/{os.environ.get('HF_REPO_CHECKPOINT')}" if repo_owner else os.environ.get("HF_REPO_CHECKPOINT")

    state_file = os.path.join(save_path, "state.json")
    if os.path.exists(state_file):
        try:
            api.upload_file(path_or_fileobj=state_file, path_in_repo="state.json",
                             repo_id=checkpoint_repo, repo_type="dataset", token=hf_token)
            print("✅ state.json final enviado")
        except Exception as e:
            print(f"⚠️ Falha ao enviar state.json: {e}")

    cp_dir = os.path.join(save_path, "checkpoints")
    if os.path.isdir(cp_dir):
        for fn in os.listdir(cp_dir):
            local = os.path.join(cp_dir, fn)
            if os.path.isfile(local):
                try:
                    api.upload_file(path_or_fileobj=local, path_in_repo=f"checkpoints/{fn}",
                                     repo_id=checkpoint_repo, repo_type="dataset", token=hf_token)
                except Exception as e:
                    print(f"⚠️ Falha checkpoint {fn}: {e}")


def upload_duckdb_if_small():
    save_path = os.environ.get("SAVE_PATH")
    max_mb = int(os.environ.get("DB_UPLOAD_MAX_SIZE_MB", "1024"))
    db_path = os.path.join(save_path, "emails.duckdb")
    if not os.path.exists(db_path):
        print("ℹ️ Sem DuckDB local")
        return

    size_mb = os.path.getsize(db_path) / (1024 * 1024)
    print(f"ℹ️ DuckDB encontrado: {size_mb:.1f} MB")
    if size_mb > max_mb:
        print(f"⚠️ DuckDB grande demais ({size_mb:.1f}MB > {max_mb}MB); pulando upload")
        return

    api, repo_owner, hf_token = get_repo_and_api()
    if not hf_token:
        return
    emails_repo = f"{repo_owner}/{os.environ.get('HF_REPO_EMAILS')}" if repo_owner else os.environ.get("HF_REPO_EMAILS")
    try:
        api.upload_file(path_or_fileobj=db_path, path_in_repo="emails.duckdb",
                         repo_id=emails_repo, repo_type="dataset", token=hf_token)
        print("✅ DuckDB enviado")
    except Exception as e:
        print(f"⚠️ Falha ao enviar DuckDB: {e}")


def evaluate_completion():
    save_path = os.environ.get("SAVE_PATH")
    state_path = os.path.join(save_path, "state.json")
    completed = False

    state = {}
    if os.path.exists(state_path):
        try:
            with open(state_path, "r", encoding="utf-8") as f:
                state = json.load(f)
        except Exception:
            state = {}

    if state.get("status") == "completed":
        completed = True

    db_path = os.path.join(save_path, "emails.duckdb")
    if not completed and os.path.exists(db_path):
        try:
            import duckdb
            db = duckdb.connect(db_path)
            rows = db.execute("""
                SELECT LOWER(SUBSTR(email, POSITION('@' IN email) + 1)) AS domain, COUNT(*)
                FROM emails_raw GROUP BY domain
            """).fetchall()
            domains = [r[0] for r in rows]

            cp_dir = os.path.join(save_path, "checkpoints")
            done = 0
            if os.path.isdir(cp_dir):
                for fn in os.listdir(cp_dir):
                    try:
                        with open(os.path.join(cp_dir, fn), "r", encoding="utf-8") as f:
                            d = json.load(f)
                            if d.get("status") == "completed":
                                done += 1
                    except Exception:
                        continue

            print(f"DOMAINS_TOTAL={len(domains)} CHECKPOINTS_COMPLETED={done}")
            if len(domains) > 0 and len(domains) == done:
                completed = True
            db.close()
        except Exception as e:
            print(f"ℹ️ Inferência via DuckDB falhou: {e}")

    if completed:
        state["status"] = "completed"
        state["completed_at"] = datetime.datetime.utcnow().isoformat() + "Z"
        tmp = state_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, ensure_ascii=False)
        os.replace(tmp, state_path)
        print("✅ status=completed gravado")

    # grava resultado para o bash ler
    with open(os.path.join(save_path, ".completion_flag"), "w") as f:
        f.write("1" if completed else "0")


def track_progress(initial_metric):
    save_path = os.environ.get("SAVE_PATH")
    state_path = os.path.join(save_path, "state.json")
    state = {}
    if os.path.exists(state_path):
        try:
            with open(state_path, "r", encoding="utf-8") as f:
                state = json.load(f)
        except Exception:
            state = {}

    final_metric = state.get("total_emails_extracted", 0) or state.get("total_processed", 0) or 0

    if final_metric <= initial_metric:
        state["runs_without_progress"] = int(state.get("runs_without_progress", 0)) + 1
        print(f"⚠️ Sem progresso -> runs_without_progress={state['runs_without_progress']}")
    else:
        state["runs_without_progress"] = 0
        print("✅ Progresso detectado, contador resetado")

    tmp = state_path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)
    os.replace(tmp, state_path)

    upload_state_and_checkpoints()


if __name__ == "__main__":
    import sys
    action = sys.argv[1] if len(sys.argv) > 1 else "all"
    initial_metric = int(sys.argv[2]) if len(sys.argv) > 2 else 0

    if action in ("all", "upload"):
        upload_state_and_checkpoints()
    if action in ("all", "duckdb"):
        upload_duckdb_if_small()
    if action in ("all", "complete"):
        evaluate_completion()
    if action == "progress":
        track_progress(initial_metric)
