package gocb

type NodeVersion struct {
	Major   int
	Minor   int
	Patch   int
	Build   int
	Edition string
}

func (v NodeVersion) Equal(ov NodeVersion) bool {
	if v.Major == ov.Major && v.Minor == ov.Minor &&
		v.Patch == ov.Patch && v.Edition == ov.Edition {
		return true
	}
	return false
}

func (v NodeVersion) Higher(ov NodeVersion) bool {
	if v.Major > ov.Major {
		return true
	} else if v.Major < ov.Major {
		return false
	}

	if v.Minor > ov.Minor {
		return true
	} else if v.Minor < ov.Minor {
		return false
	}

	if v.Patch > ov.Patch {
		return true
	} else if v.Patch < ov.Patch {
		return false
	}

	if v.Build > ov.Build {
		return true
	} else if v.Build < ov.Build {
		return false
	}

	editionMap := map[string]int{
		"Community":  1,
		"Enterprise": 2,
	}

	vEditionVal, ok := editionMap[v.Edition]
	if !ok {
		vEditionVal = 0
	}

	ovEditionVal, ok := editionMap[ov.Edition]
	if !ok {
		ovEditionVal = 0
	}

	if vEditionVal > ovEditionVal {
		return true
	}

	return false
}

func (v NodeVersion) Lower(ov NodeVersion) bool {
	return !v.Higher(ov) && !v.Equal(ov)
}
