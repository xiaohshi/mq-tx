package models

type ConfigKey struct {
	Def 	 map[string]*ConfigData `yaml:"def"`
}

type ConfigData struct {
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Host	 string `yaml:"host"`
}