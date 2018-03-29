package mns

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/go-spirit/spirit/cache"
	"github.com/go-spirit/spirit/component"
	"github.com/go-spirit/spirit/doc"
	"github.com/go-spirit/spirit/mail"
	"github.com/go-spirit/spirit/message"
	"github.com/go-spirit/spirit/worker"
	"github.com/go-spirit/spirit/worker/fbp"
	"github.com/go-spirit/spirit/worker/fbp/protocol"
	"github.com/gogap/ali_mns"
	"github.com/pborman/uuid"
	"github.com/sirupsen/logrus"
)

type mnsQueue struct {
	Name            string
	Endpoint        string
	AccessKeyId     string
	AccessKeySecret string
	Queue           ali_mns.AliMNSQueue
}

type MNSComponent struct {
	opts component.Options

	queues []mnsQueue

	endpoint        string
	accessKeyId     string
	accessKeySecret string

	maxSize   int
	blockSize int

	respChan chan ali_mns.BatchMessageReceiveResponse
	errChan  chan error

	stopC  chan bool
	stopWg *sync.WaitGroup

	alias string

	cache cache.Cache
}

func init() {
	component.RegisterComponent("mns", NewMNSComponent)
	doc.RegisterDocumenter("mns", &MNSComponent{})
}

func NewMNSComponent(alias string, opts ...component.Option) (comp component.Component, err error) {
	mnsComp := &MNSComponent{
		alias:    alias,
		stopC:    make(chan bool),
		stopWg:   &sync.WaitGroup{},
		respChan: make(chan ali_mns.BatchMessageReceiveResponse, 30),
		errChan:  make(chan error, 30),
	}

	err = mnsComp.init(opts...)
	if err != nil {
		return
	}

	comp = mnsComp

	return
}

func (p *MNSComponent) init(opts ...component.Option) (err error) {

	for _, o := range opts {
		o(&p.opts)
	}

	if p.opts.Config == nil {
		err = errors.New("mns component config is nil")
		return
	}

	cache, exist := p.opts.Caches.Require("mns")
	if !exist {
		err = fmt.Errorf("the cache of `mns` not exist")
		return
	}

	if cache.IsLocal() {
		err = fmt.Errorf("the `mns` driver could not be local")
		return
	}

	p.cache = cache

	p.maxSize = int(p.opts.Config.GetInt64("partition.max-size", 1024*30))
	p.blockSize = int(p.opts.Config.GetInt64("partition.block-size", 1000*1000*512))

	if p.blockSize <= 1000 {
		p.blockSize = 1000
	}

	p.accessKeyId = p.opts.Config.GetString("access-key-id")
	p.accessKeySecret = p.opts.Config.GetString("access-key-secret")
	p.endpoint = p.opts.Config.GetString("endpoint")

	queuesConf := p.opts.Config.GetConfig("queues")

	if queuesConf == nil {
		return
	}

	qNames := queuesConf.Keys()

	var mnsQueues []mnsQueue

	for _, name := range qNames {
		queueName := queuesConf.GetString(name+".queue", name)
		endpoint := queuesConf.GetString("endpoint", p.endpoint)
		qAkId := queuesConf.GetString("access-key-id", p.accessKeyId)
		qAkSecret := queuesConf.GetString("access-key-secret", p.accessKeySecret)

		qClient := ali_mns.NewAliMNSClient(endpoint, qAkId, qAkSecret)
		aliQueue := ali_mns.NewMNSQueue(queueName, qClient)

		q := mnsQueue{
			Name:            queueName,
			Endpoint:        endpoint,
			AccessKeyId:     qAkId,
			AccessKeySecret: qAkSecret,
			Queue:           aliQueue,
		}

		mnsQueues = append(mnsQueues, q)
	}

	p.queues = mnsQueues

	return
}

func (p *MNSComponent) Alias() string {
	if p == nil {
		return ""
	}
	return p.alias
}

func (p *MNSComponent) Start() error {

	for _, q := range p.queues {
		mgr := ali_mns.NewMNSQueueManager(q.AccessKeyId, q.AccessKeySecret)
		_, err := mgr.GetQueueAttributes(q.Endpoint, q.Name)
		if err != nil {
			return err
		}
	}

	for _, q := range p.queues {
		go q.Queue.BatchReceiveMessage(p.respChan, p.errChan, 16, 30)
	}

	go p.receiveMessage()

	return nil
}

type CacheMetadata struct {
	Md5   string   `json:"md5"`
	Parts []string `json:"parts"`
}

func (p *MNSComponent) packPayload(payload *protocol.Payload) (err error) {

	rawBody := payload.Content().GetBody()
	if len(rawBody) < p.maxSize {
		return
	}

	cacheId := uuid.New()

	metadata := &CacheMetadata{
		Md5: fmt.Sprintf("%0x", md5.Sum(rawBody)),
	}

	buf := bytes.NewBuffer(rawBody)

	for {
		next := buf.Next(p.blockSize)
		if len(next) == 0 {
			break
		}

		partId := uuid.New()
		p.cache.Set("mns.cache:data:"+partId, base64.StdEncoding.EncodeToString(next))

		metadata.Parts = append(metadata.Parts, partId)
	}

	if payload.Context == nil {
		payload.Context = make(map[string]string)
	}

	dataMetadata, err := json.Marshal(metadata)
	if err != nil {
		return
	}

	p.cache.Set("mns.cache:metadata:"+cacheId, string(dataMetadata))

	err = payload.Content().SetBody(dataMetadata)

	if err != nil {
		return
	}

	payload.Context["mns.cached"] = cacheId

	logrus.WithField("component", "mns").WithField("cache_id", cacheId).Debugln("cache packed")

	return
}

func (p *MNSComponent) unpackPayload(payload *protocol.Payload) (err error) {
	ctx := payload.GetContext()

	if ctx == nil {
		return
	}

	cacheId, exist := ctx["mns.cached"]

	if !exist {
		return
	}

	cacheV, exist := p.cache.Get("mns.cache:metadata:" + cacheId)
	if !exist {
		logrus.Debugf("mns cache time exceed, %s\n", cacheId)
		return
	}

	data := cacheV.(string)

	metadata := &CacheMetadata{}

	err = json.Unmarshal([]byte(data), &metadata)
	if err != nil {
		err = fmt.Errorf("unmashal mns cache metadata failure, id: %s", cacheId)
		return
	}

	unpackedData := bytes.NewBuffer(nil)

	for i := 0; i < len(metadata.Parts); i++ {
		partDataV, exist := p.cache.Get("mns.cache:data:" + metadata.Parts[i])
		if !exist {
			err = fmt.Errorf("get cache metdata part %d failure", i)
			return
		}

		partDataBase64, ok := partDataV.(string)
		if !ok {
			err = fmt.Errorf("convert part data to base64 string failure")
			return
		}

		var partData []byte
		partData, err = base64.StdEncoding.DecodeString(partDataBase64)
		if err != nil {
			return
		}

		_, err = unpackedData.Write(partData)
		if err != nil {
			return
		}
	}

	dataMd5 := fmt.Sprintf("%0x", md5.Sum(unpackedData.Bytes()))
	if dataMd5 != metadata.Md5 {
		err = fmt.Errorf("unpack data failure, data md5 not match")
		return
	}

	delete(ctx, "mns.cached")

	err = payload.Content().SetBody(unpackedData.Bytes())
	if err != nil {
		return
	}

	logrus.WithField("component", "mns").WithField("cache_id", cacheId).Debugln("cache unpacked")

	return
}

func (p *MNSComponent) postMessage(resp ali_mns.MessageReceiveResponse) (err error) {

	payload := &protocol.Payload{}
	err = protocol.Unmarshal([]byte(resp.MessageBody), payload)

	if err != nil {
		return
	}

	graph, exist := payload.GetGraph(payload.GetCurrentGraph())
	if !exist {
		err = fmt.Errorf("could not get graph of %s in MNSComponent.postMessage", payload.GetCurrentGraph())
		return
	}

	graph.MoveForward()

	port, err := graph.CurrentPort()

	if err != nil {
		return
	}

	fromUrl := ""
	prePort, preErr := graph.PrevPort()
	if preErr == nil {
		fromUrl = prePort.GetUrl()
	}

	err = p.unpackPayload(payload)
	if err != nil {
		return
	}

	session := mail.NewSession()

	session.WithPayload(payload)
	session.WithFromTo(fromUrl, port.GetUrl())

	fbp.SessionWithPort(session, graph.GetName(), port.GetUrl(), port.GetMetadata())

	err = p.opts.Postman.Post(
		message.NewUserMessage(session),
	)

	if err != nil {
		return
	}

	return
}

func (p *MNSComponent) receiveMessage() {
	for {
		select {
		case resp, ok := <-p.respChan:
			{
				if !ok {
					return
				}

				for _, m := range resp.Messages {
					err := p.postMessage(m)
					if err != nil {
						logrus.WithField("component", "mns").WithField("alias", p.alias).WithError(err).Errorln("post received mns message failured")
					}
				}
			}
		case err, ok := <-p.errChan:
			{
				if !ok {
					return
				}

				if !ali_mns.ERR_MNS_MESSAGE_NOT_EXIST.IsEqual(err) {
					logrus.WithField("component", "mns").WithField("alias", p.alias).WithError(err).Errorln("receive mns message failure")
				}
			}
		case <-p.stopC:
			{
				p.stopWg.Done()
				return
			}
		}
	}
}

func (p *MNSComponent) Stop() error {

	if len(p.queues) == 0 {
		return nil
	}

	logrus.WithField("component", "mns").WithField("alias", p.alias).WithField("queue-count", len(p.queues)).Infoln("stopping...")

	wg := &sync.WaitGroup{}
	wg.Add(len(p.queues))

	for _, q := range p.queues {
		go func(queue mnsQueue) {
			defer wg.Done()
			logrus.WithField("component", "mns").WithField("alias", p.alias).WithField("queue", queue.Name).Infoln("stopping...")
			queue.Queue.Stop()
			logrus.WithField("component", "mns").WithField("alias", p.alias).WithField("queue", queue.Name).Infoln("stopped.")
		}(q)
	}

	wg.Wait()

	p.stopWg.Add(1)

	p.stopC <- true

	p.stopWg.Wait()

	close(p.errChan)
	close(p.respChan)
	close(p.stopC)

	logrus.WithField("component", "mns").WithField("alias", p.alias).Infoln("all queues listener stopped.")

	return nil
}

// It is a send out func
func (p *MNSComponent) sendMessage(session mail.Session) (err error) {

	logrus.WithField("component", "mns").WithField("To", session.To()).Debugln("send message")

	fbp.BreakSession(session)

	port := fbp.GetSessionPort(session)

	if port == nil {
		err = errors.New("port info not exist")
		return
	}

	queueName := session.Query("queue")

	if len(queueName) == 0 {
		err = fmt.Errorf("queue name is empty")
		return
	}

	endpoint, exist := port.Metadata["endpoint"]

	if !exist {
		endpoint = p.endpoint
	}

	akId, exist := port.Metadata["access-key-id"]
	if !exist {
		akId = p.accessKeyId
	}

	akSecret, exist := port.Metadata["access-key-secret"]

	if !exist {
		akSecret = p.accessKeySecret
	}

	if len(endpoint) == 0 || len(akId) == 0 || len(akSecret) == 0 {
		err = fmt.Errorf("error mns send params in msn component, port to url: %s", port.Url)
		return
	}

	client := ali_mns.NewAliMNSClient(endpoint, akId, akSecret)
	queue := ali_mns.NewMNSQueue(queueName, client)

	payload, ok := session.Payload().Interface().(*protocol.Payload)
	if !ok {
		err = errors.New("could not convert session payload to *protocol.Payload")
		return
	}

	err = p.packPayload(payload)
	if err != nil {
		return
	}

	data, err := payload.ToBytes()

	if err != nil {
		return
	}

	req := ali_mns.MessageSendRequest{
		MessageBody: data,
		Priority:    8,
	}

	_, err = queue.SendMessage(req)

	if err != nil {
		return
	}

	return
}

func (p *MNSComponent) Route(mail.Session) worker.HandlerFunc {
	return p.sendMessage
}

func (p *MNSComponent) Document() doc.Document {

	document := doc.Document{
		Title:       "MNS Sender And Receiver",
		Description: "MNS is aliyun message service, we could receive queue message from msn and send message to msn queue",
	}

	return document
}
