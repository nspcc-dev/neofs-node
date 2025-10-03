package object

import (
	"errors"
	"fmt"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
)

func verifyRegularECPart(part object.Object, policyRules []netmap.ECRule) error {
	parent := part.Parent()
	if parent == nil {
		return errors.New("missing parent header")
	}

	if parentVal, partVal := parent.Version(), part.Version(); !equalProtoVersions(parentVal, partVal) {
		return fmt.Errorf("diff proto version in parent (%s) and part (%s)", stringifyVersion(parentVal), stringifyVersion(partVal))
	}
	if parentVal, partVal := parent.GetContainerID(), part.GetContainerID(); parentVal != partVal {
		return fmt.Errorf("diff container in parent (%s) and part (%s)", parentVal, partVal)
	}
	if parentVal, partVal := parent.Owner(), part.Owner(); parentVal != partVal {
		return fmt.Errorf("diff owner in parent (%s) and part (%s)", parentVal, partVal)
	}
	if parentVal, partVal := parent.CreationEpoch(), part.CreationEpoch(); parentVal != partVal {
		return fmt.Errorf("diff creation epoch in parent (%s) and part (%s)", parentVal, partVal)
	}
	if parentVal, partVal := parent.SessionToken() != nil, part.SessionToken() != nil; parentVal != partVal {
		return fmt.Errorf("diff session token presence in parent (%t) and part (%t)", parentVal, partVal)
	}
	_, parentHH := parent.PayloadHomomorphicHash()
	_, partHH := part.PayloadHomomorphicHash()
	if parentHH != partHH {
		return fmt.Errorf("diff homomorphic hash presence in parent (%t) and part (%t)", parentHH, partHH)
	}

	pi, err := iec.GetRequiredPartInfo(part)
	if err != nil {
		return fmt.Errorf("unavailable part info: %w", err)
	}
	if pi.RuleIndex >= len(policyRules) {
		return fmt.Errorf("rule index attribute %d overflows total number of rules %d in policy", pi.RuleIndex, len(policyRules))
	}

	rule := policyRules[pi.RuleIndex]
	dataPartNum := rule.DataPartNum()
	if total := dataPartNum + rule.ParityPartNum(); pi.Index >= int(total) {
		return fmt.Errorf("part index attribute %d overflows total number of parts %d in policy", pi.Index, total)
	}

	parentPldLen := parent.PayloadSize()
	partPldLen := part.PayloadSize()

	if exp := (parentPldLen + uint64(dataPartNum) - 1) / uint64(dataPartNum); exp != partPldLen {
		return fmt.Errorf("part payload len %d is not equal to expected %d for parent %d", partPldLen, exp, parentPldLen)
	}

	return nil
}
