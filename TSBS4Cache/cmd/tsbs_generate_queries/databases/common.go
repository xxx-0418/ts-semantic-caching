package databases

func PanicIfErr(err error) {
	if err != nil {
		panic(err.Error())
	}
}
