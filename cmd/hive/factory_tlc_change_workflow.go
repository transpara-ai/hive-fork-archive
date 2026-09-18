package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/transpara-ai/eventgraph/go/pkg/types"
	"github.com/transpara-ai/hive/pkg/hive"
	"github.com/transpara-ai/hive/pkg/hive/factoryv1"
)

// cmdFactoryTLCChangeWorkflow is the bounded Civilization intake-to-report
// entrypoint. It persists exact source/invocation/report records but performs
// no repository or GitHub continuation effect.
func cmdFactoryTLCChangeWorkflow(args []string) error {
	fs := flag.NewFlagSet("factory tlc-change-workflow", flag.ContinueOnError)
	human := fs.String("human", "", "Operator name (required)")
	storeDSN := fs.String("store", "", "Store DSN (postgres://... or empty for DATABASE_URL/in-memory)")
	invocationPath := fs.String("invocation", "", "Exact tlc-change-continuation/v1 invocation JSON path, or - for stdin (required)")
	identityPath := fs.String("identity", "", "Configured exact TLC workflow identity JSON path (required)")
	pluginRoot := fs.String("plugin-root", "", "Absolute installed TLC plugin root (required)")
	runnerPath := fs.String("runner", "", "Absolute installed-skill runner executable (required)")
	timeout := fs.Duration("timeout", 15*time.Minute, "Bounded installed-skill runner timeout")
	runnerArgs := repeatedStringFlag{}
	fs.Var(&runnerArgs, "runner-arg", "Argument passed directly to --runner (repeatable)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("%w: factory tlc-change-workflow accepts no positional arguments", errUsage)
	}
	if err := requireFlags([]requiredFlag{
		{name: "--human", val: *human},
		{name: "--invocation", val: *invocationPath},
		{name: "--identity", val: *identityPath},
		{name: "--plugin-root", val: *pluginRoot},
		{name: "--runner", val: *runnerPath},
	}); err != nil {
		return err
	}
	invocationJSON, err := readBoundedTLCInput(*invocationPath, 16<<20)
	if err != nil {
		return err
	}
	invocation, err := factoryv1.DecodeContinuationInvocation(invocationJSON)
	if err != nil {
		return err
	}
	expected, err := readTLCWorkflowIdentity(*identityPath)
	if err != nil {
		return err
	}
	runner, err := factoryv1.NewCommandTLCChangeWorkflowRunner(*pluginRoot, *runnerPath, runnerArgs, *timeout)
	if err != nil {
		return err
	}
	ctx := context.Background()
	fc, err := openFactoryContext(ctx, *storeDSN, *human)
	if err != nil {
		return err
	}
	defer fc.close()
	conversation, err := types.NewConversationID("conv_tlc_change_" + invocation.SourceChain.HeadDigest[:32])
	if err != nil {
		return err
	}
	events, err := hive.NewFactoryV1EventGraphStore(fc.store, fc.factory, fc.signer, fc.humanID, conversation)
	if err != nil {
		return err
	}
	workStore, err := hive.NewFactoryV1WorkStore(fc.store, fc.factory, fc.signer, fc.humanID, conversation)
	if err != nil {
		return err
	}
	observedAt, err := time.Parse(time.RFC3339, invocation.SourceChain.Records[len(invocation.SourceChain.Records)-1].ObservedTime)
	if err != nil {
		return fmt.Errorf("parse exact source observed_time: %w", err)
	}
	actor := factoryv1.Principal{Kind: "human", StableID: fc.humanID.Value(), SubjectRef: fc.humanID.Value()}
	result, err := factoryv1.RunContinuationWorkflow(
		ctx, events, workStore, runner, expected, invocationJSON, actor,
		"hive:"+fc.humanID.Value(), observedAt,
	)
	if err != nil {
		return err
	}
	_, err = os.Stdout.Write(append(append([]byte(nil), result.ExactReportJSON...), '\n'))
	return err
}

func readTLCWorkflowIdentity(path string) (factoryv1.TLCWorkflowIdentity, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return factoryv1.TLCWorkflowIdentity{}, fmt.Errorf("read configured TLC workflow identity: %w", err)
	}
	var identity factoryv1.TLCWorkflowIdentity
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&identity); err != nil {
		return factoryv1.TLCWorkflowIdentity{}, fmt.Errorf("decode configured TLC workflow identity: %w", err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return factoryv1.TLCWorkflowIdentity{}, errors.New("configured TLC workflow identity has trailing JSON")
	}
	return identity, nil
}

func readBoundedTLCInput(path string, limit int64) ([]byte, error) {
	var reader io.Reader
	if path == "-" {
		reader = os.Stdin
	} else {
		file, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("open TLC workflow invocation: %w", err)
		}
		defer file.Close()
		reader = file
	}
	content, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, fmt.Errorf("read TLC workflow invocation: %w", err)
	}
	if int64(len(content)) > limit {
		return nil, errors.New("TLC workflow invocation exceeds the bounded input limit")
	}
	return content, nil
}
