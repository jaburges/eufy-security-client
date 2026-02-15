/**
 * WebRTC streaming support for Eufy devices that use WebRTC for live video
 * (e.g., T85V0 Lock WiFi Video cameras).
 *
 * Architecture:
 * 1. P2P channel sends CMD_START_REALTIME_MEDIA → camera responds SUCCESS
 * 2. This service connects to the signaling server (WebSocket)
 * 3. Exchanges SDP offer/answer and ICE candidates
 * 4. Establishes WebRTC peer connection
 * 5. Receives video/audio RTP tracks
 * 6. Extracts raw H.264/H.265 frames and emits them
 *
 * Protocol reverse-engineered from Tuya ThingP2PSDK native library.
 * Signaling uses JSON commands: {"cmd":"...", "args":{...}}
 */

import { TypedEmitter } from "tiny-typed-emitter";
import WebSocket from "ws";
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

/**
 * Signaling protocol commands discovered from libThingP2PSDK.so:
 *
 * Client -> Server:
 *   {"cmd":"reset","args":{"local_id":"<id>"}}
 *   {"cmd":"set_remote_online","args":{"remote_id":"<device_id>"}}
 *   {"cmd":"pre_connect","args":{"remote_id":"<id>","dev_id":"<id>","token":...,"connect_session":"<id>"}}
 *   {"cmd":"connect","args":{"remote_id":"<id>","token":...,"trace_id":"<id>","timeout_ms":N,"lan_mode":N,"connect_session":"<id>"}}
 *
 * Server -> Client:
 *   {"cmd":"signaling_result","args":{"code":N,"remote_id":"<id>","signaling":"<sdp/ice json>"}}
 *   {"cmd":"http_result","args":{"api":"<name>","code":N,"result":"<json>"}}
 */

interface SignalingMessage {
  cmd: string;
  args: Record<string, unknown>;
}

export class WebRTCStream extends TypedEmitter<WebRTCStreamEvents> {
  private ws: WebSocket | null = null;
  private pc: RTCPeerConnection | null = null;
  private config: WebRTCConfig;
  private connectSession: string;
  private connected = false;
  private stopped = false;

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
      // Step 1: Create the RTCPeerConnection
      this.createPeerConnection();

      // Step 2: Connect to signaling server
      await this.connectSignaling();

      // Step 3: Create and send SDP offer
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

    if (this.ws) {
      try {
        this.sendSignaling({ cmd: "close", args: { handle: "0", reason: 0, is_force: 1 } });
        this.ws.close();
      } catch (_e) { /* ignore */ }
      this.ws = null;
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
        // Use signaling servers as potential TURN/STUN
        ...this.config.signalingServers
          .filter((s) => !s.startsWith("https://"))
          .map((s) => ({ urls: s })),
      ],
    });

    // Add receive-only transceiver for video and audio
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
      if (candidate && this.ws?.readyState === WebSocket.OPEN) {
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
      // Extract raw video payload from RTP packet
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

  // ---- Signaling ----

  private connectSignaling(): Promise<void> {
    return new Promise((resolve, reject) => {
      const serverUrl = this.config.signalingServers[0];
      if (!serverUrl) {
        reject(new Error("No signaling servers available"));
        return;
      }

      // Convert HTTPS to WSS for WebSocket connection
      const wsUrl = serverUrl.replace("https://", "wss://").replace("http://", "ws://");

      rootP2PLogger.info(`WebRTC connecting to signaling server`, {
        stationSN: this.config.stationSN,
        url: wsUrl,
      });

      const timeout = setTimeout(() => {
        reject(new Error("Signaling server connection timeout"));
      }, 10000);

      const ws = new WebSocket(wsUrl, {
        rejectUnauthorized: false,
        headers: {
          "User-Agent": "okhttp/3.12.1",
        },
      });
      this.ws = ws;

      ws.on("open", () => {
        clearTimeout(timeout);
        rootP2PLogger.info(`WebRTC signaling connected`, {
          stationSN: this.config.stationSN,
          url: wsUrl,
        });

        // Send initial authentication/registration
        this.sendSignaling({
          cmd: "reset",
          args: { local_id: this.config.adminUserId },
        });

        this.sendSignaling({
          cmd: "set_remote_online",
          args: { remote_id: this.config.p2pDid },
        });

        resolve();
      });

      ws.on("message", (data: WebSocket.Data) => {
        this.handleSignalingMessage(data);
      });

      ws.on("error", (error: Error) => {
        clearTimeout(timeout);
        rootP2PLogger.error(`WebRTC signaling error`, {
          stationSN: this.config.stationSN,
          error: error.message,
        });
        reject(error);
      });

      ws.on("close", (code: number, reason: Buffer) => {
        rootP2PLogger.info(`WebRTC signaling closed`, {
          stationSN: this.config.stationSN,
          code,
          reason: reason.toString(),
        });
        if (!this.stopped) {
          this.stop();
        }
      });
    });
  }

  private sendSignaling(message: SignalingMessage): void {
    if (this.ws?.readyState === WebSocket.OPEN) {
      const payload = JSON.stringify(message);
      rootP2PLogger.debug(`WebRTC signaling send`, {
        stationSN: this.config.stationSN,
        cmd: message.cmd,
        payload: payload.substring(0, 500),
      });
      this.ws.send(payload);
    }
  }

  private handleSignalingMessage(data: WebSocket.Data): void {
    try {
      const raw = data.toString();
      rootP2PLogger.info(`WebRTC signaling received`, {
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
          rootP2PLogger.info(`WebRTC signaling unknown cmd`, {
            stationSN: this.config.stationSN,
            cmd: message.cmd,
            args: message.args,
          });
      }
    } catch (error) {
      rootP2PLogger.error(`WebRTC signaling message parse error`, {
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

    // Send pre_connect first to register the session
    this.sendSignaling({
      cmd: "pre_connect",
      args: {
        remote_id: this.config.p2pDid,
        dev_id: this.config.deviceSN,
        token: { webrtc_auth: this.config.adminUserId },
        connect_session: this.connectSession,
      },
    });

    // Then send the SDP offer as signaling data
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

    // Also try the connect command
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
