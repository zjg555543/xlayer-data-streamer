package datastreamer

import (
	"encoding/binary"
	"fmt"

	"github.com/0xPolygonHermez/zkevm-data-streamer/log"
)

var (
	// ErrEndBookmarkInvalidParamToBookmark is returned when the end bookmark is invalid, param to bookmark
	ErrEndBookmarkInvalidParamToBookmark = fmt.Errorf("end bookmark invalid param to bookmark")
)

const (
	EtBatchEnd   = 0x4 // EtBatchEnd is entry type for batch end
	EtL2BlockEnd = 0x6 // EtL2BlockEnd is entry type for L2 block end
)

// handleRangeBookmarkCommand processes the CmdRangeBookmark command
func (s *StreamServer) handleRangeBookmarkCommand(cli *client) error {
	if cli.status != csStopped {
		log.Error("Stream to client already started!")
		_ = s.sendResultEntry(uint32(CmdErrAlreadyStarted), StrCommandErrors[CmdErrAlreadyStarted], cli)
		return ErrClientAlreadyStarted
	}

	cli.status = csSyncing
	err := s.processCmdRangeBookmark(cli)
	if err == nil {
		cli.status = csStopped
	}

	return err
}

func (s *StreamServer) processCmdRangeBookmark(client *client) error {
	// Read start and end bookmark parameter
	sb, err := readBookmark(client)
	if err != nil {
		return err
	}
	eb, err := readBookmark(client)
	if err != nil {
		return err
	}
	log.Debugf("Client %s command RangeBookmark start: [%v], end [%v]", client.clientID, sb, eb)

	from, err := s.bookmark.GetBookmark(sb)
	if err != nil || from >= s.nextEntry {
		log.Errorf("RangeBookmark command invalid start bookmark %v for client %s: %v", sb, client.clientID, err)
		err = ErrStartBookmarkInvalidParamFromBookmark
		_ = s.sendResultEntry(uint32(CmdErrBadFromBookmark), StrCommandErrors[CmdErrBadFromBookmark], client)
		return err
	}

	to, err := s.bookmark.GetBookmark(eb)
	if err != nil || to == 0 || to >= s.nextEntry {
		log.Errorf("RangeBookmark command invalid end bookmark %v for client %s: %v", eb, client.clientID, err)
		err = ErrEndBookmarkInvalidParamToBookmark
		_ = s.sendResultEntry(uint32(CmdErrBadToBookmark), StrCommandErrors[CmdErrBadToBookmark], client)
		return err
	}

	// Find L2BlockEnd or BatchEnd entry number
	iterator, err := s.streamFile.iteratorFrom(to, true)
	if err != nil {
		log.Errorf("RangeBookmark command invalid to iterator %v for client %s: %v", eb, client.clientID, err)
		err = ErrEndBookmarkInvalidParamToBookmark
		_ = s.sendResultEntry(uint32(CmdErrBadToBookmark), StrCommandErrors[CmdErrBadToBookmark], client)
		return err
	}
	reachedBlockEnd := false
	for {
		// Get next entry data
		end, err := s.streamFile.iteratorNext(iterator)
		if err != nil {
			log.Warnf("RangeBookmark command invalid next iterator %v for client %s: %v", eb, client.clientID, err)
			err = ErrEndBookmarkInvalidParamToBookmark
			_ = s.sendResultEntry(uint32(CmdErrBadToBookmark), StrCommandErrors[CmdErrBadToBookmark], client)
			return err
		}

		// Check if end of iterator or reached end of the range
		if end || (reachedBlockEnd && iterator.Entry.Type != EtBatchEnd) {
			break
		}

		if iterator.Entry.Type == EtL2BlockEnd {
			// Reached the end of a block. Continue iteration to check for batch end
			reachedBlockEnd = true
			to = iterator.Entry.Number
		} else if iterator.Entry.Type == EtBatchEnd {
			// Reached the end of a batch
			to = iterator.Entry.Number
			break
		}
	}

	// Send a command result entry OK
	err = s.sendResultEntry(0, "OK", client)
	if err != nil {
		return err
	}

	// Send toEntry
	be := make([]byte, 8)
	binary.BigEndian.PutUint64(be, to)
	TimeoutWrite(client, be, s.writeTimeout)

	return s.streamingRangeEntry(client, from, to)
}

// streamingRangeEntry streams the range of file entries from entry number until to entry number
func (s *StreamServer) streamingRangeEntry(client *client, fromEntry uint64, toEntry uint64) error {
	if fromEntry > toEntry {
		return ErrInvalidBookmarkRange
	}

	log.Debugf("SYNCING %s from entry %d to entry %d...", client.clientID, fromEntry, toEntry)

	// Start file stream iterator
	iterator, err := s.streamFile.iteratorFrom(fromEntry, true)
	if err != nil {
		return err
	}

	// Loop data entries from file stream iterator
	for {
		// Get next entry data
		end, err := s.streamFile.iteratorNext(iterator)
		if err != nil {
			return err
		}

		// Check if end of iterator
		if end {
			break
		}

		// Send the file data entry
		binaryEntry := encodeFileEntryToBinary(iterator.Entry)
		log.Debugf("Sending data entry %d (type %d) to %s", iterator.Entry.Number, iterator.Entry.Type, client.clientID)
		if client.conn != nil {
			_, err = TimeoutWrite(client, binaryEntry, s.writeTimeout)
		} else {
			err = ErrNilConnection
		}
		if err != nil {
			log.Errorf("Error sending entry %d to %s: %v", iterator.Entry.Number, client.clientID, err)
			return err
		}

		if iterator.Entry.Number == toEntry {
			break
		}
	}
	log.Debugf("Synced %s until %d!", client.clientID, iterator.Entry.Number)

	// Close iterator
	s.streamFile.iteratorEnd(iterator)

	return nil
}

func readBookmark(client *client) ([]byte, error) {
	// Read bookmark length parameter
	length, err := readFullUint32(client)
	if err != nil {
		return nil, err
	}

	// Check maximum length allowed
	if length > maxBookmarkLength {
		log.Errorf("Client %s exceeded [%d] maximum allowed length [%d] for a bookmark.",
			client.clientID, length, maxBookmarkLength)
		return nil, ErrBookmarkMaxLength
	}

	// Read start bookmark parameter
	bookmark, err := readFullBytes(length, client)
	if err != nil {
		return nil, err
	}

	return bookmark, nil
}
