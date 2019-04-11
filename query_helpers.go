package cworm

//New ...
func (db *DB) New(Model interface{}) (interface{}, error) {
	return db.Insert(Model)
}
