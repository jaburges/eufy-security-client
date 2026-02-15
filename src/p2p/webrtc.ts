/**
 * WebRTC streaming support for Eufy devices that use WebRTC for live video
 * (e.g., T85V0 Lock WiFi Video cameras).
 *
 * Architecture:
 * 1. P2P channel sends CMD_START_REALTIME_MEDIA → camera responds SUCCESS
 * 2. This service connects to the signaling server via MQTT over WebSocket
 * 3. Exchanges SDP offer/answer and ICE candidates over MQTT topics
 * 4. Establishes WebRTC peer connection
 * 5. Receives video/audio RTP tracks
 * 6. Extracts raw H.264/H.265 frames and emits them
 *
 * Protocol based on Tuya ThingP2PSDK / IoT Hub signaling over MQTT.
 * Signaling uses JSON commands: {"cmd":"...", "args":{...}}
 */

import { TypedEmitter } from "tiny-typed-emitter";
import * as mqtt from "mqtt";
import {
  RTCPeerConnection,
  RTCSessionDescription,
  RTCIceCandidate,
  type MediaStreamTrack,
} from "werift";
import { rootP2PLogger } from "../logging";

export interface WebRTCStreamEvents {
  "video data": (data: Buffer) => void;
  "audio data": (data: Buffer) => void;
  "stream started": () => void;
  "stream stopped": () => void;
  "error": (error: Error) => void;
}

export interface WebRTCConfig {
  signalingServers: string[];
  stationSN: string;
  deviceSN: string;
  adminUserId: string;
  /** P2P DID for the device */
  p2pDid: string;
}

interface SignalingMessage {
  cmd: string;
  args: Record<string, unknown>;
}

export class WebRTCStream extends TypedEmitter<WebRTCStreamEvents> {
  private mqttClient: mqtt.MqttClient | null = null;
  private pc: RTCPeerConnection | null = null;
  private config: WebRTCConfig;
  private connectSession: string;
  private connected = false;
  private stopped = false;

  /** MQTT topic to publish signaling messages to the device */
  private publishTopic = "";
  /** MQTT topic to subscribe to receive responses */
  private subscribeTopic = "";

  constructor(config: WebRTCConfig) {
    super();
    this.config = config;
    this.connectSession = this.generateSessionId();
  }

  private generateSessionId(): string {
    return `es_${Date.now()}_${Math.random().toString(36).substring(2, 10)}`;
  }

  /**
   * Start the WebRTC stream. Call this after CMD_START_REALTIME_MEDIA returns SUCCESS.
   */
  public async start(): Promise<void> {
    rootP2PLogger.info(`WebRTC stream starting`, {
      stationSN: this.config.stationSN,
      deviceSN: this.config.deviceSN,
      signalingServers: this.config.signalingServers,
    });

    try {
      this.createPeerConnection();
      await this.connectSignaling();
      await this.sendOffer();
    } catch (error) {
      rootP2PLogger.error(`WebRTC stream start failed`, {
        stationSN: this.config.stationSN,
        error: (error as Error).message,
      });
      this.emit("error", error as Error);
      this.stop();
    }
  }

  /**
   * Stop the WebRTC stream and clean up resources.
   */
  public stop(): void {
    if (this.stopped) return;
    this.stopped = true;

    rootP2PLogger.info(`WebRTC stream stopping`, {
      stationSN: this.config.stationSN,
    });

    if (this.mqttClient) {
      try {
        if (this.mqttClient.connected) {
          this.sendSignaling({ cmd: "close", args: { handle: "0", reason: 0, is_force: 1 } });
        }
        this.mqttClient.removeAllListeners();
        this.mqttClient.end(true);
      } catch (_e) { /* ignore */ }
      this.mqttClient = null;
    }

    if (this.pc) {
      try {
        this.pc.close();
      } catch (_e) { /* ignore */ }
      this.pc = null;
    }

    this.connected = false;
    this.emit("stream stopped");
  }

  public isStreaming(): boolean {
    return this.connected && !this.stopped;
  }

  // ---- Peer Connection ----

  private createPeerConnection(): void {
    rootP2PLogger.debug(`WebRTC creating peer connection`, {
      stationSN: this.config.stationSN,
    });

    this.pc = new RTCPeerConnection({
      iceServers: [
        { urls: "stun:stun.l.google.com:19302" },
      ],
    });

    this.pc.addTransceiver("video", { direction: "recvonly" });
    this.pc.addTransceiver("audio", { direction: "recvonly" });

    this.pc.ontrack = (event: { track: MediaStreamTrack; transceiver: any }) => {
      rootP2PLogger.info(`WebRTC track received`, {
        stationSN: this.config.stationSN,
        kind: event.track.kind,
        id: event.track.id,
      });

      if (event.track.kind === "video") {
        this.handleVideoTrack(event.track);
      } else if (event.track.kind === "audio") {
        this.handleAudioTrack(event.track);
      }
    };

    this.pc.onIceCandidate.subscribe((candidate) => {
      if (candidate && this.mqttClient?.connected) {
        rootP2PLogger.debug(`WebRTC sending ICE candidate`, {
          stationSN: this.config.stationSN,
          candidate: candidate.candidate,
        });
        this.sendSignaling({
          cmd: "signaling_result",
          args: {
            code: 0,
            remote_id: this.config.p2pDid,
            signaling: JSON.stringify({
              type: "candidate",
              candidate: candidate.toJSON(),
            }),
          },
        });
      }
    });

    this.pc.iceConnectionStateChange.subscribe((state) => {
      rootP2PLogger.info(`WebRTC ICE connection state changed`, {
        stationSN: this.config.stationSN,
        state: state,
      });

      if (state === "connected" || state === "completed") {
        this.connected = true;
        this.emit("stream started");
      } else if (state === "failed" || state === "disconnected" || state === "closed") {
        if (this.connected) {
          this.connected = false;
          this.stop();
        }
      }
    });
  }

  private handleVideoTrack(track: MediaStreamTrack): void {
    rootP2PLogger.info(`WebRTC video track handler attached`, {
      stationSN: this.config.stationSN,
    });

    track.onReceiveRtp.subscribe((rtp) => {
      const payload = Buffer.from(rtp.payload);
      if (payload.length > 0) {
        this.emit("video data", payload);
      }
    });
  }

  private handleAudioTrack(track: MediaStreamTrack): void {
    rootP2PLogger.info(`WebRTC audio track handler attached`, {
      stationSN: this.config.stationSN,
    });

    track.onReceiveRtp.subscribe((rtp) => {
      const payload = Buffer.from(rtp.payload);
      if (payload.length > 0) {
        this.emit("audio data", payload);
      }
    });
  }

  // ---- MQTT Signaling ----

  /**
   * Connect to the signaling server using MQTT over WebSocket.
   * Tuya-based devices use MQTT for WebRTC signaling exchange.
   * We try multiple connection strategies:
   *   1. MQTT over WSS at /mqtt path
   *   2. Plain WSS (fallback)
   */
  private connectSignaling(): Promise<void> {
    return new Promise((resolve, reject) => {
      const serverUrl = this.config.signalingServers[0];
      if (!serverUrl) {
        reject(new Error("No signaling servers available"));
        return;
      }

      // Derive MQTT topics based on Tuya convention:
      //   publish:   /av/u/<userId>/ipc  (to device via moto service)
      //   subscribe: /av/u/<userId>/ipc  (responses from device)
      // We use a simple topic structure that can be refined once we see server responses.
      const userId = this.config.adminUserId;
      const deviceId = this.config.deviceSN;
      this.publishTopic = `/av/u/${userId}/ipc`;
      this.subscribeTopic = `/av/u/${userId}/ipc`;

      // Try MQTT over WSS connection
      const mqttUrl = serverUrl.replace("https://", "wss://").replace("http://", "ws://") + "/mqtt";

      rootP2PLogger.info(`WebRTC connecting to MQTT signaling server`, {
        stationSN: this.config.stationSN,
        mqttUrl,
        publishTopic: this.publishTopic,
        subscribeTopic: this.subscribeTopic,
      });

      const connectTimeout = setTimeout(() => {
        reject(new Error("MQTT signaling server connection timeout (15s)"));
      }, 15000);

      const clientId = `eufy_${userId.substring(0, 8)}_${Date.now()}`;

      this.mqttClient = mqtt.connect(mqttUrl, {
        clientId,
        username: userId,
        password: this.config.p2pDid,
        protocolVersion: 4,
        clean: true,
        connectTimeout: 12000,
        rejectUnauthorized: false,
        wsOptions: {
          headers: {
            "User-Agent": "okhttp/3.12.1",
          },
        },
      });

      this.mqttClient.on("connect", () => {
        clearTimeout(connectTimeout);
        rootP2PLogger.info(`WebRTC MQTT signaling connected`, {
          stationSN: this.config.stationSN,
          clientId,
        });

        // Subscribe to response topic
        this.mqttClient?.subscribe(this.subscribeTopic, { qos: 1 }, (err) => {
          if (err) {
            rootP2PLogger.error(`WebRTC MQTT subscribe failed`, {
              stationSN: this.config.stationSN,
              topic: this.subscribeTopic,
              error: err.message,
            });
          } else {
            rootP2PLogger.info(`WebRTC MQTT subscribed to topic`, {
              stationSN: this.config.stationSN,
              topic: this.subscribeTopic,
            });
          }
        });

        // Send initial registration messages
        this.sendSignaling({
          cmd: "reset",
          args: { local_id: userId },
        });

        this.sendSignaling({
          cmd: "set_remote_online",
          args: { remote_id: this.config.p2pDid },
        });

        resolve();
      });

      this.mqttClient.on("message", (_topic: string, payload: Buffer) => {
        this.handleSignalingMessage(payload);
      });

      this.mqttClient.on("error", (error: Error) => {
        clearTimeout(connectTimeout);
        rootP2PLogger.error(`WebRTC MQTT signaling error`, {
          stationSN: this.config.stationSN,
          error: error.message,
        });
        reject(error);
      });

      this.mqttClient.on("close", () => {
        rootP2PLogger.info(`WebRTC MQTT signaling closed`, {
          stationSN: this.config.stationSN,
        });
        if (!this.stopped) {
          this.stop();
        }
      });

      this.mqttClient.on("offline", () => {
        rootP2PLogger.info(`WebRTC MQTT signaling went offline`, {
          stationSN: this.config.stationSN,
        });
      });
    });
  }

  private sendSignaling(message: SignalingMessage): void {
    if (!this.mqttClient?.connected) return;

    const payload = JSON.stringify(message);
    rootP2PLogger.debug(`WebRTC MQTT signaling send`, {
      stationSN: this.config.stationSN,
      topic: this.publishTopic,
      cmd: message.cmd,
      payload: payload.substring(0, 500),
    });

    this.mqttClient.publish(this.publishTopic, payload, { qos: 1 });
  }

  private handleSignalingMessage(data: Buffer): void {
    try {
      const raw = data.toString();
      rootP2PLogger.info(`WebRTC MQTT signaling received`, {
        stationSN: this.config.stationSN,
        data: raw.substring(0, 500),
      });

      const message: SignalingMessage = JSON.parse(raw);

      switch (message.cmd) {
        case "signaling_result":
          this.handleSignalingResult(message.args);
          break;
        case "http_result":
          this.handleHttpResult(message.args);
          break;
        case "set_remote_online":
          rootP2PLogger.debug(`WebRTC remote device online`, {
            stationSN: this.config.stationSN,
            args: message.args,
          });
          break;
        default:
          rootP2PLogger.info(`WebRTC MQTT signaling unknown cmd`, {
            stationSN: this.config.stationSN,
            cmd: message.cmd,
            args: message.args,
          });
      }
    } catch (error) {
      rootP2PLogger.error(`WebRTC MQTT signaling message parse error`, {
        stationSN: this.config.stationSN,
        error: (error as Error).message,
        data: data.toString().substring(0, 200),
      });
    }
  }

  private async handleSignalingResult(args: Record<string, unknown>): Promise<void> {
    const signalingStr = args.signaling as string;
    if (!signalingStr || !this.pc) return;

    try {
      const signaling = JSON.parse(signalingStr);

      if (signaling.type === "answer") {
        rootP2PLogger.info(`WebRTC received SDP answer`, {
          stationSN: this.config.stationSN,
        });
        const answer = new RTCSessionDescription(signaling.sdp, "answer");
        await this.pc.setRemoteDescription(answer);
      } else if (signaling.type === "offer") {
        rootP2PLogger.info(`WebRTC received SDP offer (device-initiated)`, {
          stationSN: this.config.stationSN,
        });
        const offer = new RTCSessionDescription(signaling.sdp, "offer");
        await this.pc.setRemoteDescription(offer);
        const answer = await this.pc.createAnswer();
        await this.pc.setLocalDescription(answer);
        this.sendSignaling({
          cmd: "signaling_result",
          args: {
            code: 0,
            remote_id: this.config.p2pDid,
            signaling: JSON.stringify({
              type: "answer",
              sdp: this.pc.localDescription?.sdp,
            }),
          },
        });
      } else if (signaling.type === "candidate" && signaling.candidate) {
        rootP2PLogger.debug(`WebRTC received ICE candidate`, {
          stationSN: this.config.stationSN,
        });
        const candidate = new RTCIceCandidate(signaling.candidate);
        await this.pc.addIceCandidate(candidate);
      }
    } catch (error) {
      rootP2PLogger.error(`WebRTC signaling result processing error`, {
        stationSN: this.config.stationSN,
        error: (error as Error).message,
      });
    }
  }

  private handleHttpResult(args: Record<string, unknown>): void {
    rootP2PLogger.info(`WebRTC HTTP result`, {
      stationSN: this.config.stationSN,
      api: args.api,
      code: args.code,
    });
  }

  // ---- SDP Offer ----

  private async sendOffer(): Promise<void> {
    if (!this.pc) return;

    rootP2PLogger.info(`WebRTC creating SDP offer`, {
      stationSN: this.config.stationSN,
    });

    const offer = await this.pc.createOffer();
    await this.pc.setLocalDescription(offer);

    this.sendSignaling({
      cmd: "pre_connect",
      args: {
        remote_id: this.config.p2pDid,
        dev_id: this.config.deviceSN,
        token: { webrtc_auth: this.config.adminUserId },
        connect_session: this.connectSession,
      },
    });

    this.sendSignaling({
      cmd: "signaling_result",
      args: {
        code: 0,
        remote_id: this.config.p2pDid,
        signaling: JSON.stringify({
          type: "offer",
          sdp: this.pc.localDescription?.sdp,
        }),
      },
    });

    this.sendSignaling({
      cmd: "connect",
      args: {
        remote_id: this.config.p2pDid,
        token: { webrtc_auth: this.config.adminUserId },
        trace_id: this.connectSession,
        timeout_ms: 30000,
        lan_mode: 1,
        connect_session: this.connectSession,
      },
    });
  }
}
