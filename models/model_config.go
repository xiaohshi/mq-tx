package models

type ConfigData struct {
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Host	 string `yaml:"host"`
}