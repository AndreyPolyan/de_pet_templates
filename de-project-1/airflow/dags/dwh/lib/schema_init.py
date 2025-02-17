import os
from logging import Logger
from pathlib import Path
from lib import PgConnect

class SchemaDdl:
    """Handles schema migrations by applying SQL scripts to a Vertica database."""

    def __init__(self, db: PgConnect, log: Logger) -> None:
        """
        Initialize SchemaDdl with a database connection and logger.

        Args:
            db (PgConnect): Database connection instance.
            log (Logger): Logger instance for logging migration progress.
        """
        self._db = db
        self.log = log

    def init_schema(self, path_to_scripts: str) -> None:
        """
        Apply schema migration scripts from the specified directory.

        Args:
            path_to_scripts (str): Path to the directory containing SQL migration scripts.

        Raises:
            Exception: If a script execution fails, the exception is logged and re-raised.
        """
        files = [fp for fp in os.listdir(path_to_scripts) if fp.endswith(".sql")]
        file_paths = [Path(path_to_scripts, f) for f in files]
        file_paths.sort(key=lambda x: x.name)

        self.log.info(f"Found {len(file_paths)} files to check changes.")
        self.log.info("Starting the migration process...")

        cached_dir = os.path.join(path_to_scripts, "cached")
        if not os.path.exists(cached_dir):
            os.makedirs(cached_dir)

        for i, fp in enumerate(file_paths, start=1):
            self.log.info(f"Iteration {i}. Applying file {fp.name}")
            script = fp.read_text()
            ext_script = ""
            new_fp = None

            try:
                ext_fp = Path(cached_dir, fp.name)
                ext_script = ext_fp.read_text()
            except Exception:
                if not os.path.exists(os.path.join(cached_dir, fp.name)):
                    new_fp = Path(cached_dir, fp.name)
                    new_fp.write_text(script)
                else:
                    raise

            if ext_script.split("---OLD SCRIPT---")[0] != script:
                try:
                    with self._db.connection() as conn:
                        with conn.cursor() as cur:
                            cur.execute(script)
                    if ext_script:
                        ext_fp.write_text(script + "\n---OLD SCRIPT---\n/*" + ext_script + "*/")
                except Exception as e:
                    if new_fp:
                        new_fp.unlink()
                    self.log.error(f"Failed to run migration: {fp.name}")
                    self.log.error(f"Failed script:\n{script}")
                    raise e

                self.log.info(f"Iteration {i}. File {fp.name} processed successfully.")
            else:
                self.log.info(f"Iteration {i}. File {fp.name} - No changes found. Skipping...")

        self.log.info("Migration process is finished. Exiting...")