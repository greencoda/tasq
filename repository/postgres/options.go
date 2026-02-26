package postgres

type Option func(*Repository) func(r *Repository) *Repository

// WithSchemaName allows you to specify a schema name for the tasks table and task status type.
func WithSchemaName(schemaName string) Option {
	return func(r *Repository) func(r *Repository) *Repository {
		return func(r *Repository) *Repository {
			r.schemaName = &schemaName
			return r
		}
	}
}

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
			return r
		}
	}
}
