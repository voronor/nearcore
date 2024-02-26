use clap::Parser;
use near_store::db::Database;
use near_store::metadata::DbKind;
use near_store::{DBCol, Mode, NodeStorage, StoreConfig, Temperature};
use rayon::iter::IntoParallelIterator;
use rayon::iter::ParallelIterator;
use std::path::{Path, PathBuf};
use strum::IntoEnumIterator;

/// This can potentially support db specified not in config, but in command line.
/// `ChangeRelative { path: Path, archive: bool }`
/// But it is a pain to implement, because of all the current storage possibilities.
/// So, I'll leave it as a TODO(posvyatokum): implement relative path DbSelector.
/// This can be useful workaround for config modification.
#[derive(clap::Subcommand)]
enum DbSelector {
    ChangeHot,
    ChangeCold,
}

#[derive(clap::Args)]
pub(crate) struct ChangeDbKindCommand {
    /// Desired DbKind.
    #[clap(long)]
    new_kind: DbKind,
    /// Which db to change.
    #[clap(subcommand)]
    db_selector: DbSelector,
}

impl ChangeDbKindCommand {
    pub(crate) fn run(&self, home_dir: &Path) -> anyhow::Result<()> {
        let near_config = nearcore::config::load_config(
            &home_dir,
            near_chain_configs::GenesisValidationMode::UnsafeFast,
        )?;
        let opener = NodeStorage::opener(
            home_dir,
            near_config.config.archive,
            &near_config.config.store,
            near_config.config.cold_store.as_ref(),
        );

        let storage = opener.open()?;
        let store = match self.db_selector {
            DbSelector::ChangeHot => storage.get_hot_store(),
            DbSelector::ChangeCold => {
                storage.get_cold_store().ok_or(anyhow::anyhow!("No cold store"))?
            }
        };
        Ok(store.set_db_kind(self.new_kind)?)
    }
}

#[derive(Parser)]
pub(crate) struct PourDbCommand {
    /// Source db path
    #[clap(long)]
    source_db: PathBuf,
    /// Target db path
    #[clap(long)]
    target_db: PathBuf,
    /// Number of threads to use in the migration
    #[clap(long, default_value_t = 1)]
    num_threads: usize,
    /// Batch size to use
    #[clap(long, default_value_t = 500_000_000)]
    batch_size: usize,
}

impl PourDbCommand {
    pub(crate) fn run(&self, home_dir: &Path) -> anyhow::Result<()> {
        let source_db = Self::open_rocks_db(home_dir, &self.source_db, near_store::Mode::ReadOnly)?;
        let target_db =
            Self::open_rocks_db(home_dir, &self.target_db, near_store::Mode::ReadWrite)?;

        let columns = DBCol::iter().collect::<Vec<DBCol>>();
        let num_threads = self.num_threads;
        let batch_size = self.batch_size;

        rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build()
            .map_err(|_| anyhow::anyhow!("/Failed to create rayon pool"))?
            .install(|| {
                columns
                    .into_par_iter() // Process every cold column as a separate task in thread pool in parallel.
                    // Copy column to cold db.
                    .map(|col: DBCol| -> anyhow::Result<()> {
                        tracing::info!(target: "pour_db", ?col, "Started column migration");
                        let mut transaction = near_store::cold_storage::BatchTransaction::new(
                            target_db.clone(),
                            batch_size,
                        );
                        for result in source_db.iter(col) {
                            let (key, value) = result?;
                            transaction.set_and_write_if_full(col, key.to_vec(), value.to_vec())?;
                        }
                        transaction.write()?;
                        tracing::info!(target: "pour_db", ?col, "Finished column migration");
                        Ok(())
                    })
                    // Return first found error, or Ok(())
                    .reduce(
                        || Ok(()), // Ok(()) by default
                        // First found Err, or Ok(())g
                        |left, right| -> anyhow::Result<()> {
                            vec![left, right]
                                .into_iter()
                                .filter(|res| res.is_err())
                                .next()
                                .unwrap_or(Ok(()))
                        },
                    )
            })?;
        Ok(())
    }

    fn open_rocks_db(
        home_dir: &Path,
        db_path: &Path,
        mode: near_store::Mode,
    ) -> anyhow::Result<std::sync::Arc<dyn Database>> {
        let db_path =
            if db_path.is_absolute() { PathBuf::from(db_path) } else { home_dir.join(&db_path) };
        let config = StoreConfig::default();
        Ok(std::sync::Arc::new(near_store::db::RocksDB::open(
            &db_path,
            &config,
            mode,
            Temperature::Hot,
        )?))
    }
}

#[derive(Parser)]
pub(crate) struct DeleteColumnCommand {
    /// Db path
    #[clap(long)]
    db: PathBuf,
    /// Column to delete
    #[clap(long)]
    column: DBCol,
}
impl DeleteColumnCommand {
    pub(crate) fn run(&self, home_dir: &Path) -> anyhow::Result<()> {
        let db = {
            let db_path = if self.db.is_absolute() {
                PathBuf::from(&self.db)
            } else {
                home_dir.join(&self.db)
            };
            let config = StoreConfig::default();
            std::sync::Arc::new(near_store::db::RocksDB::open(
                &db_path,
                &config,
                Mode::ReadWrite,
                Temperature::Hot,
            )?)
        };

        let mut transaction = near_store::db::DBTransaction::new();
        transaction.delete_all(self.column);
        db.write(transaction)?;

        Ok(())
    }
}
