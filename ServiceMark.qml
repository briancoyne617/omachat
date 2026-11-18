import QtQuick
import QtQuick.Shapes
import qs.Commons

// A small monochrome mark naming the service (and, for Google Messages, the
// transport) a conversation travels over. Everything is drawn in `color`, so
// it follows the row's foreground. The glyphs are simplified originals, not
// the vendors' logos. SMS has no glyph; it gets a tiny "SMS" pill instead.
Item {
  id: root

  property string network: ""
  property string kind: ""
  property color color: "white"
  property string fontFamily: Style.font.family
  property real size: Style.space(16)

  readonly property bool isSms: network === "gmessages" && kind === "sms"
  readonly property string glyph: isSms ? "" : (network === "whatsapp" ? "whatsapp"
    : (network === "messenger" ? "messenger" : (network === "gmessages" ? "gmessages" : "")))

  implicitWidth: Math.round(size * 1.25)
  implicitHeight: size
  Accessible.role: Accessible.Graphic
  Accessible.name: isSms ? "SMS" : (network === "gmessages" ? "Google Messages RCS"
    : (network === "whatsapp" ? "WhatsApp" : (network === "messenger" ? "Messenger" : network)))

  // Glyph artwork lives on a 24 x 24 grid, scaled to `size`.
  component Glyph: Shape {
    required property string name
    visible: parent.glyph === name
    width: 24
    height: 24
    x: Math.round((parent.width - parent.size) / 2)
    y: Math.round((parent.height - parent.size) / 2)
    scale: parent.size / 24
    transformOrigin: Item.TopLeft
    preferredRendererType: Shape.CurveRenderer
  }

  // WhatsApp: speech bubble outline with a handset inside.
  Glyph {
    name: "whatsapp"
    ShapePath {
      strokeColor: root.color
      strokeWidth: 2
      fillColor: "transparent"
      capStyle: ShapePath.RoundCap
      joinStyle: ShapePath.RoundJoin
      PathSvg { path: "M3.5 20.5 L5.1 16.2 A8.6 8.6 0 1 1 8.1 19 Z" }
    }
    ShapePath {
      strokeColor: "transparent"
      fillColor: root.color
      PathSvg { path: "M9 8 C8.5 8.6 8.5 9.8 9.6 11.6 C10.8 13.5 12.5 14.8 14.3 15.3 C15.4 15.6 16 15 16.3 14.4 L16.3 13.6 L14.7 12.8 L13.9 13.5 C13 13.1 11.9 12 11.5 11.1 L12.2 10.3 L11.4 8.3 L9.9 8 Z" }
    }
  }

  // Messenger: rounded bubble with a lightning zigzag.
  Glyph {
    name: "messenger"
    ShapePath {
      strokeColor: root.color
      strokeWidth: 2
      fillColor: "transparent"
      capStyle: ShapePath.RoundCap
      joinStyle: ShapePath.RoundJoin
      PathSvg { path: "M12 3 C7 3 3 6.6 3 11.3 C3 13.9 4.2 16.1 6.1 17.6 L6.1 21 L9.3 19.3 C10.1 19.5 11 19.6 12 19.6 C17 19.6 21 16 21 11.3 C21 6.6 17 3 12 3 Z" }
    }
    ShapePath {
      strokeColor: root.color
      strokeWidth: 1
      fillColor: root.color
      joinStyle: ShapePath.RoundJoin
      PathSvg { path: "M6.4 13.7 L10.2 9.4 L12.9 11.9 L17.6 9 L13.8 13.3 L11.1 10.8 Z" }
    }
  }

  // Google Messages: rounded bubble with three lines.
  Glyph {
    name: "gmessages"
    ShapePath {
      strokeColor: root.color
      strokeWidth: 2
      fillColor: "transparent"
      capStyle: ShapePath.RoundCap
      joinStyle: ShapePath.RoundJoin
      PathSvg { path: "M6 4 H18 A3 3 0 0 1 21 7 V14 A3 3 0 0 1 18 17 H11 L7 20.5 V17 H6 A3 3 0 0 1 3 14 V7 A3 3 0 0 1 6 4 Z" }
    }
    ShapePath {
      strokeColor: root.color
      strokeWidth: 1.8
      fillColor: "transparent"
      capStyle: ShapePath.RoundCap
      PathSvg { path: "M8 8 H16 M8 10.7 H16 M8 13.4 H13" }
    }
  }

  Rectangle {
    objectName: "serviceMarkSms"
    visible: root.isSms
    anchors.centerIn: parent
    width: root.implicitWidth
    height: Math.round(root.size * 0.68)
    radius: height / 2
    color: "transparent"
    border.width: 1
    border.color: root.color

    Text {
      textFormat: Text.PlainText
      anchors.centerIn: parent
      text: "SMS"
      color: root.color
      font.family: root.fontFamily
      font.bold: true
      font.pixelSize: Math.max(6, Math.round(root.size * 0.42))
    }
  }
}
