package gocb

import (
	"time"
)

func (suite *UnitTestSuite) TestThresholdGroup() {
	time.Sleep(100 * time.Millisecond)

	var grp thresholdLogGroup
	grp.init("Test", 2*time.Millisecond, 3)
	grp.recordOp(&thresholdLogSpan{duration: 1 * time.Millisecond})
	grp.recordOp(&thresholdLogSpan{duration: 2 * time.Millisecond})

	if len(grp.ops) != 1 {
		suite.T().Fatalf("Failed to ignore duration below threshold")
	}

	grp.recordOp(&thresholdLogSpan{duration: 6 * time.Millisecond})
	grp.recordOp(&thresholdLogSpan{duration: 4 * time.Millisecond})
	grp.recordOp(&thresholdLogSpan{duration: 5 * time.Millisecond})
	grp.recordOp(&thresholdLogSpan{duration: 2 * time.Millisecond})
	grp.recordOp(&thresholdLogSpan{duration: 9 * time.Millisecond})

	if len(grp.ops) != 3 {
		suite.T().Fatalf("Failed to reach real capacity")
	}
	if grp.ops[0].duration != 5*time.Millisecond {
		suite.T().Fatalf("Failed to insert in correct order (1)")
	}
	if grp.ops[1].duration != 6*time.Millisecond {
		suite.T().Fatalf("Failed to insert in correct order (2)")
	}
	if grp.ops[2].duration != 9*time.Millisecond {
		suite.T().Fatalf("Failed to insert in correct order (3)")
	}
}
