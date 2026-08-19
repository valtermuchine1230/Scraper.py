import os
from huggingface_hub import HfApi


def upload_all():
    save_path = os.environ.get("SAVE_PATH")
    hf_token = os.environ.get("HF_TOKEN")

    if not hf_token or not save_path:
        return

    api = HfApi()
    repo_owner = None
    try:
        who = api.whoami(token=hf_token)
        repo_owner = who.get("name") or who.get("user")
    except Exception:
        pass

    checkpoint_repo = f"{repo_owner}/{os.environ.get('HF_REPO_CHECKPOINT')}" if repo_owner else os.environ.get("HF_REPO_CHECKPOINT")

    state_file = os.path.join(save_path, "state.json")
    if os.path.exists(state_file):
        try:
            api.upload_file(
                path_or_fileobj=state_file,
                path_in_repo="state.json",
                repo_id=checkpoint_repo,
                repo_type="dataset",
                token=hf_token,
            )
            print("✅ state.json sincronizado")
        except Exception as e:
            print(f"⚠️ Falha ao sincronizar state.json: {e}")

    cp_dir = os.path.join(save_path, "checkpoints")
    if os.path.isdir(cp_dir):
        for fn in os.listdir(cp_dir):
            local = os.path.join(cp_dir, fn)
            if os.path.isfile(local):
                try:
                    api.upload_file(
                        path_or_fileobj=local,
                        path_in_repo=f"checkpoints/{fn}",
                        repo_id=checkpoint_repo,
                        repo_type="dataset",
                        token=hf_token,
                    )
                    print(f"✅ {fn} sincronizado")
                except Exception as e:
                    print(f"⚠️ Falha ao subir {fn}: {e}")


if __name__ == "__main__":
    upload_all()
