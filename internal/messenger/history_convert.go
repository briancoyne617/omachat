package messenger

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix"
	"go.mau.fi/mautrix-meta/pkg/messagix/securehistory"
	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/whatsmeow"
	armadillo "go.mau.fi/whatsmeow/proto"
	"go.mau.fi/whatsmeow/proto/waArmadilloApplication"
	"go.mau.fi/whatsmeow/proto/waArmadilloBackupMessage"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waConsumerApplication"
	"go.mau.fi/whatsmeow/proto/waMsgApplication"
	waTypes "go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

// archiveMessage is one archived message in the form OmaChat stores. Live
// encrypted messages are converted by the same fbContent and
// fbMessageRestricted helpers, so a restored message looks like a received one.
type archiveMessage struct {
	id          string
	sender      int64
	timestampMS int64
	text        string
	media       *messengerMedia
	restricted  bool
	reactions   map[int64]string
}

// decodeArchiveMessage turns an archived protobuf into the message a live
// connection would have delivered. A nil result means "nothing to show": a
// locally generated event or an admin notice. Ported from mautrix-meta
// connector.decodeArchiveMessage.
func decodeArchiveMessage(chat waTypes.JID, self int64, message *waArmadilloBackupMessage.BackupMessage, timestampMS int64) (*events.FBMessage, error) {
	var decoded events.FBMessage
	switch payload := message.GetPayload().(type) {
	case *waArmadilloBackupMessage.BackupMessage_EncryptedTransportMessage:
		var application waMsgApplication.MessageApplication
		if err := proto.Unmarshal(payload.EncryptedTransportMessage, &application); err != nil {
			return nil, fmt.Errorf("invalid archived Messenger application: %w", err)
		}
		var err error
		decoded, err = whatsmeow.DecodeFBApplication(&application)
		if err != nil {
			return nil, err
		}
	case *waArmadilloBackupMessage.BackupMessage_EncryptedTransportEvent,
		*waArmadilloBackupMessage.BackupMessage_EncryptedTransportLocallyTransformedMessage,
		*waArmadilloBackupMessage.BackupMessage_MiTransportAdminMessage:
		return nil, nil
	default:
		return nil, errors.New("missing Messenger archive message payload")
	}
	metadata := message.GetMetadata()
	if metadata.GetMessageID() == "" || metadata.GetSenderID() == "" || timestampMS < 0 {
		return nil, errors.New("missing Messenger archive message metadata")
	}
	sender, err := strconv.ParseInt(metadata.GetSenderID(), 10, 64)
	if err != nil || sender <= 0 {
		return nil, errors.New("invalid Messenger archive sender")
	}
	decoded.Info = waTypes.MessageInfo{
		MessageSource: waTypes.MessageSource{
			Chat:     chat,
			Sender:   waTypes.NewJID(metadata.GetSenderID(), waTypes.MessengerServer),
			IsFromMe: sender == self,
			IsGroup:  chat.Server == waTypes.GroupServer,
		},
		ID:        metadata.GetMessageID(),
		Timestamp: time.UnixMilli(timestampMS),
	}
	return &decoded, nil
}

// archiveMediaPath accepts the CDN path an archive attachment carries. The
// download path must be a plain absolute path: whatsmeow joins it to a media
// host it chose, so anything that could name another host is ignored.
func archiveMediaPath(path string) bool {
	return strings.HasPrefix(path, "/") && !strings.HasPrefix(path, "//") &&
		!strings.ContainsAny(path, "\\\r\n\x00 ") && len(path) <= 4096
}

// convertArchiveMessage ports mautrix-meta connector.convertArchiveMessage:
// it folds an archived message with its reaction and edit supplementals into
// one displayable message.
func convertArchiveMessage(chat waTypes.JID, self int64, message *securehistory.DecryptedMessage) (*archiveMessage, error) {
	if message == nil || message.Message == nil {
		return nil, errors.New("missing decrypted Messenger archive message")
	}
	parent, err := decodeArchiveMessage(chat, self, message.Message.Message, message.Message.TimestampMS)
	if err != nil || parent == nil {
		return nil, err
	}
	// Only plain content is a message. Reactions, edits, revokes and signals
	// at the top level carry no text of their own.
	if consumer := parent.GetConsumerApplication(); consumer != nil {
		payload := consumer.GetPayload()
		content := payload.GetContent()
		if content == nil || content.GetReactionMessage() != nil || content.GetEditMessage() != nil || content.GetPollUpdateMessage() != nil {
			return nil, nil
		}
	} else if armadilloContent := parent.GetArmadillo().GetPayload().GetContent(); armadilloContent == nil {
		return nil, nil
	}
	type archiveReaction struct {
		timestampMS int64
		text        string
	}
	reactions := make(map[string]archiveReaction)
	var latestEdit *waConsumerApplication.ConsumerApplication_EditMessage
	var editTimestampMS int64
	for _, supplemental := range message.Supplementals {
		if supplemental == nil {
			return nil, errors.New("missing Messenger archive supplemental")
		}
		switch supplemental.Message.GetPayload().(type) {
		case *waArmadilloBackupMessage.BackupMessage_EncryptedTransportEvent,
			*waArmadilloBackupMessage.BackupMessage_EncryptedTransportLocallyTransformedMessage,
			*waArmadilloBackupMessage.BackupMessage_MiTransportAdminMessage:
			continue
		}
		if supplemental.Message.GetMetadata() == nil || supplemental.Message.GetMetadata().TimestampMS == nil {
			return nil, errors.New("missing Messenger archive supplemental metadata")
		}
		timestampMS := supplemental.Message.GetMetadata().GetTimestampMS()
		decoded, err := decodeArchiveMessage(chat, self, supplemental.Message, timestampMS)
		if err != nil {
			return nil, err
		} else if decoded == nil {
			continue
		}
		content := decoded.GetConsumerApplication().GetPayload().GetContent()
		if reaction := content.GetReactionMessage(); reaction != nil {
			if target := reaction.GetKey().GetID(); target != "" && target != parent.Info.ID {
				return nil, errors.New("archive reaction references a different message")
			}
			actor := decoded.Info.Sender.User
			previous, exists := reactions[actor]
			if !exists || timestampMS > previous.timestampMS || (timestampMS == previous.timestampMS && reaction.GetText() == "") {
				reactions[actor] = archiveReaction{timestampMS: timestampMS, text: reaction.GetText()}
			}
		} else if edit := content.GetEditMessage(); edit != nil {
			if target := edit.GetKey().GetID(); target != "" && target != parent.Info.ID {
				return nil, errors.New("archive edit references a different message")
			} else if decoded.Info.Sender != parent.Info.Sender || edit.GetMessage() == nil {
				return nil, errors.New("invalid Messenger archive edit")
			}
			if latestEdit == nil || timestampMS > editTimestampMS {
				latestEdit = edit
				editTimestampMS = timestampMS
			}
		}
		// Anything else (poll updates, action notices) has no counterpart in
		// OmaChat. mautrix-meta rejects the whole message for an unknown
		// supplemental; here the message is kept without it.
	}
	if latestEdit != nil {
		parent.Message = &waConsumerApplication.ConsumerApplication{
			Payload: &waConsumerApplication.ConsumerApplication_Payload{
				Payload: &waConsumerApplication.ConsumerApplication_Payload_Content{
					Content: &waConsumerApplication.ConsumerApplication_Content{
						Content: &waConsumerApplication.ConsumerApplication_Content_MessageText{MessageText: latestEdit.GetMessage()},
					},
				},
			},
		}
	}
	text, media := fbContent(parent)
	restricted := fbMessageRestricted(parent)
	if media != nil && media.fbTransport != nil && len(message.Attachments) > 0 {
		// The archive names each attachment's current CDN path by object ID.
		for _, attachment := range message.Attachments {
			if attachment.ObjectID != media.objectID || media.objectID == "" || !archiveMediaPath(attachment.URL) {
				continue
			}
			transport := proto.CloneOf(media.fbTransport)
			transport.DirectPath = proto.String(attachment.URL)
			media.fbTransport = transport
			break
		}
	}
	out := &archiveMessage{
		id:          parent.Info.ID,
		sender:      parseUser(parent.Info.Sender.User),
		timestampMS: parent.Info.Timestamp.UnixMilli(),
		text:        text,
		media:       media,
		restricted:  restricted,
	}
	for actor, reaction := range reactions {
		if reaction.text == "" {
			continue
		}
		if out.reactions == nil {
			out.reactions = make(map[int64]string)
		}
		out.reactions[parseUser(actor)] = reaction.text
	}
	return out, nil
}

// fetchOpenThreadPage asks Messenger for one more page of the unencrypted
// thread that preceded an encrypted chat. mautrix-meta requests the same
// task and collects the answer from the live connection; the reply is also
// returned directly here, as for any other Messenger thread.
func (b *Backend) fetchOpenThreadPage(ctx context.Context, cli *messagix.Client, open int64, oldest *wireMessageRef) error {
	var task socket.Task
	if oldest != nil && oldest.id != "" {
		task = &socket.FetchMessagesTask{
			ThreadKey: open, Direction: 0, ReferenceTimestampMs: oldest.timestampMS, ReferenceMessageId: oldest.id,
			SyncGroup: 1, Cursor: cli.GetCursor(1),
		}
	} else {
		task = &socket.CreateThreadTask{ThreadFBID: open, SyncGroup: 1}
	}
	resp, err := cli.ExecuteTasks(ctx, task)
	if err != nil {
		return err
	}
	b.handleTable(resp)
	return nil
}

// marshalSentFBMessage reproduces the transport bytes whatsmeow sent, which
// the secure-history upload encrypts and authenticates with the franking key.
func marshalSentFBMessage(message armadillo.RealMessageApplicationSub, metadata *waMsgApplication.MessageApplication_Metadata) ([]byte, error) {
	var subproto waMsgApplication.MessageApplication_SubProtocolPayload
	subproto.FutureProof = waCommon.FutureProofBehavior_PLACEHOLDER.Enum()
	switch typed := message.(type) {
	case *waConsumerApplication.ConsumerApplication:
		payload, err := proto.Marshal(typed)
		if err != nil {
			return nil, err
		}
		subproto.SubProtocol = &waMsgApplication.MessageApplication_SubProtocolPayload_ConsumerMessage{
			ConsumerMessage: &waCommon.SubProtocol{Payload: payload, Version: proto.Int32(whatsmeow.FBConsumerMessageVersion)},
		}
	case *waArmadilloApplication.Armadillo:
		payload, err := proto.Marshal(typed)
		if err != nil {
			return nil, err
		}
		subproto.SubProtocol = &waMsgApplication.MessageApplication_SubProtocolPayload_Armadillo{
			Armadillo: &waCommon.SubProtocol{Payload: payload, Version: proto.Int32(whatsmeow.FBArmadilloMessageVersion)},
		}
	default:
		return nil, fmt.Errorf("unsupported message type %T", message)
	}
	return proto.Marshal(&waMsgApplication.MessageApplication{
		Payload: &waMsgApplication.MessageApplication_Payload{
			Content: &waMsgApplication.MessageApplication_Payload_SubProtocol{SubProtocol: &subproto},
		},
		Metadata: metadata,
	})
}

// secureHistoryUploadEnabled reports the user's opt-in. It is off by default.
func (b *Backend) secureHistoryUploadEnabled() bool {
	return b.config != nil && b.config.Get().MessengerSecureHistoryUpload
}

// uploadSentMessageToHistory uploads the encrypted copy of a message this
// desktop just sent to the account's secure-storage backup, so the message
// also appears when the user restores history elsewhere. It does nothing
// unless the user turned the option on, a backup device is enrolled, and the
// chat is a one-to-one Messenger chat; mautrix-meta (PR #505) applies the
// same conditions through its e2ee_backfill option. It never blocks sending.
func (b *Backend) uploadSentMessageToHistory(chat waTypes.JID, messageID string, message armadillo.RealMessageApplicationSub, metadata *waMsgApplication.MessageApplication_Metadata) {
	if !b.secureHistoryUploadEnabled() || chat.Server != waTypes.MessengerServer || metadata == nil || len(metadata.GetFrankingKey()) == 0 {
		return
	}
	device := b.confirmedDevice()
	service := b.historyClient()
	b.mu.RLock()
	e2ee, self, parent := b.e2eeClient, b.selfID, b.runCtx
	b.mu.RUnlock()
	if device == nil || service == nil || !service.CanUpload() || e2ee == nil || e2ee.Store == nil || e2ee.Store.IdentityKey == nil {
		return
	}
	identity := e2ee.Store.IdentityKey
	if parent == nil {
		parent = context.Background()
	}
	log := b.log.With().Str("action", "upload message to encrypted history").Logger()
	go func() {
		ctx, cancel := context.WithTimeout(log.WithContext(parent), historyUploadTimeout)
		defer cancel()
		if err := doUploadSentMessage(ctx, service, device.BackupID, self, identity.Priv, identity.Pub, chat, messageID, message, metadata); err != nil {
			log.Warn().Err(err).Msg("Failed to upload sent message to encrypted history")
		} else {
			log.Debug().Msg("Uploaded sent message to encrypted history")
		}
	}()
}

func doUploadSentMessage(
	ctx context.Context, service historyService, backupID string, self int64, identityPriv, identityPub *[32]byte,
	chat waTypes.JID, messageID string, message armadillo.RealMessageApplicationSub, metadata *waMsgApplication.MessageApplication_Metadata,
) error {
	otherUserID, err := strconv.ParseInt(chat.User, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid chat ID: %w", err)
	}
	otid, err := strconv.ParseInt(messageID, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid message ID: %w", err)
	}
	transportBytes, err := marshalSentFBMessage(message, metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal sent message: %w", err)
	}
	franking := hmac.New(sha256.New, metadata.GetFrankingKey())
	franking.Write(transportBytes)
	return service.UploadSentMessage(ctx, &securehistory.MinosSentMessage{
		BackupID:       backupID,
		SenderID:       self,
		OtherUserID:    otherUserID,
		MessageID:      messageID,
		TimestampMS:    otid >> 22,
		TransportBytes: transportBytes,
		FrankingTag:    franking.Sum(nil),
		IdentityPriv:   identityPriv,
		IdentityPub:    identityPub,
	})
}
