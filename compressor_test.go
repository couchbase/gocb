package gocb

type maybeCompressedResponseValue struct {
	compressed   []byte
	uncompressed []byte
}

func (c *maybeCompressedResponseValue) GetContentCompressed() []byte {
	return c.compressed
}

func (c *maybeCompressedResponseValue) GetContentUncompressed() []byte {
	return c.uncompressed
}

func (suite *UnitTestSuite) TestCompressor() {
	suite.Run("Value should compress and decompress", func() {
		var val []byte
		for i := 0; i < 500; i++ {
			val = append(val, byte(i))
		}
		compressor := &compressor{
			CompressionEnabled:  true,
			CompressionMinRatio: 1.0,
			CompressionMinSize:  5,
		}

		after, isCompressed := compressor.Compress(val)
		suite.Assert().True(isCompressed)
		suite.Assert().NotEqual(val, after)

		decompressed, err := compressor.Decompress(&maybeCompressedResponseValue{compressed: after})
		suite.Require().NoError(err)
		suite.Assert().Equal(val, decompressed)
	})

	suite.Run("Value too small", func() {
		var val []byte
		for i := 0; i < 2; i++ {
			val = append(val, byte(i))
		}
		compressor := &compressor{
			CompressionEnabled:  true,
			CompressionMinRatio: 1.0,
			CompressionMinSize:  5,
		}

		after, isCompressed := compressor.Compress(val)
		suite.Assert().False(isCompressed)
		suite.Assert().Equal(val, after)
	})

	suite.Run("Value above ratio", func() {
		var val []byte
		for i := 0; i < 50; i++ {
			val = append(val, byte(i))
		}
		compressor := &compressor{
			CompressionEnabled:  true,
			CompressionMinRatio: 0.05,
			CompressionMinSize:  5,
		}

		after, isCompressed := compressor.Compress(val)
		suite.Assert().False(isCompressed)
		suite.Assert().Equal(val, after)
	})

	suite.Run("Compression disabled", func() {
		var val []byte
		for i := 0; i < 500; i++ {
			val = append(val, byte(i))
		}
		compressor := &compressor{
			CompressionEnabled:  false,
			CompressionMinRatio: 1.0,
			CompressionMinSize:  5,
		}

		after, isCompressed := compressor.Compress(val)
		suite.Assert().False(isCompressed)
		suite.Assert().Equal(val, after)
	})

	suite.Run("Decompress uncompressed value", func() {
		var val []byte
		for i := 0; i < 500; i++ {
			val = append(val, byte(i))
		}

		compressor := &compressor{}

		decompressed, err := compressor.Decompress(&maybeCompressedResponseValue{uncompressed: val})
		suite.Require().NoError(err)
		suite.Assert().Equal(val, decompressed)
	})
}
