package postgres

type Option func(*Repository) func(r *Repository) *Repository

// WithTablePrefix allows you to specify a prefix for the tasks table name.
func WithTableName(tableName string) Option {
	return func(r *Repository) func(r *Repository) *Repository {
		return func(r *Repository) *Repository {
			r.tableName = tableName
			return r
		}
	}
}

// WithTypePrefix allows you to specify a prefix for the task status type name.
func WithTypePrefix(prefix string) Option {
	return func(r *Repository) func(r *Repository) *Repository {
		return func(r *Repository) *Repository {
			r.statusTypeName = statusTypeName(prefix)
			if prefix != "" {
				r.tableName = prefix + "_" + "tasks"
			}
			return r
		}
	}
}
