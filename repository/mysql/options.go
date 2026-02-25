package mysql

type Option func(*Repository) func(r *Repository) *Repository

// WithTableName allows you to specify a prefix for the tasks table name.
func WithTableName(tableName string) Option {
	return func(r *Repository) func(r *Repository) *Repository {
		return func(r *Repository) *Repository {
			r.tableName = tableName
			return r
		}
	}
}
