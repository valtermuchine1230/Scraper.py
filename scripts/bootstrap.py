import os
import sys
from huggingface_hub import HfApi, hf_hub_download

def main():
    save_path = os.environ.get("SAVE_PATH", "/tmp")
    hf_token = os.environ.get("HF_TOKEN", "")
    hf_repo_checkpoint = os.environ.get("HF_REPO_CHECKPOINT", "minerador_checkpoints")

    if not hf_token:
        print("⚠️ HF_TOKEN não configurado")
        return

    api = HfApi()
    repo_owner = None
    try:
        who = api.whoami(token=hf_token)
        repo_owner = who.get("name") or who.get("user")
    except Exception as e:
        print(f"⚠️ HF whoami falhou: {e}")

    checkpoint_repo = f"{repo_owner}/{hf_repo_checkpoint}" if repo_owner else hf_repo_checkpoint

    try:
        hf_hub_download(
            repo_id=checkpoint_repo,
            filename="state.json",
            local_dir=save_path,
            token=hf_token,
            repo_type="dataset",
        )
        print("✅ state.json baixado")
    except Exception as e:
        print(f"ℹ️ Sem state.json remoto: {e}")

    try:
        files = api.list_repo_files(repo_id=checkpoint_repo, token=hf_token, repo_type="dataset")
        if "emails.duckdb" in files:
            try:
                path = hf_hub_download(
                    repo_id=checkpoint_repo,
                    filename="emails.duckdb",
                    local_dir=save_path,
                    token=hf_token,
                    repo_type="dataset",
                )
                print(f"✅ DuckDB baixado: {path}")
            except Exception as e:
                print(f"⚠️ Falha ao baixar DuckDB: {e}")
        else:
            print("ℹ️ Sem emails.duckdb no repo de checkpoint")
    except Exception as e:
        print(f"ℹ️ Não foi possível listar arquivos do repo: {e}")


if __name__ == "__main__":
    main()
